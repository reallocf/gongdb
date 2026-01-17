use sqllogictest::{DBOutput, DefaultColumnType, Normalizer};
use rusqlite::Connection;
use async_trait::async_trait;
use itertools::Itertools;
use regex::Regex;

pub struct SQLiteDB {
    conn: Connection,
}

#[async_trait]
impl sqllogictest::AsyncDB for SQLiteDB {
    type Error = rusqlite::Error;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        // Check if it's a query (SELECT) or a statement
        let sql_upper = sql.trim().to_uppercase();
        
        if sql_upper.starts_with("SELECT") || sql_upper.starts_with("WITH") {
            // It's a query - return results
            let mut stmt = self.conn.prepare(sql)?;
            let column_count = stmt.column_count();
            let mut rows = stmt.query([])?;
            
            let mut all_rows = Vec::new();
            while let Some(row) = rows.next()? {
                let mut values = Vec::new();
                for i in 0..column_count {
                    let value: String = match row.get(i) {
                        Ok(v) => v,
                        Err(_) => {
                            // Try to get as different types
                            if let Ok(v) = row.get::<_, Option<i64>>(i) {
                                v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string())
                            } else if let Ok(v) = row.get::<_, Option<f64>>(i) {
                                v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string())
                            } else {
                                "NULL".to_string()
                            }
                        }
                    };
                    values.push(value);
                }
                all_rows.push(values);
            }
            
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text; column_count],
                rows: all_rows,
            })
        } else {
            // It's a statement - execute it
            self.conn.execute(sql, [])?;
            Ok(DBOutput::StatementComplete(0))
        }
    }

    async fn shutdown(&mut self) {
        // SQLite connection will be closed when dropped
    }
}

impl SQLiteDB {
    pub fn new() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        Ok(SQLiteDB { conn })
    }
}

/// Custom validator that auto-detects valuewise vs rowwise format and handles hash-based comparison
/// 
/// If the expected results have the same number of lines as the total number of values
/// (rows * columns) and there are multiple columns, it assumes valuewise format and flattens the actual results.
/// Otherwise, it uses the default rowwise format.
/// 
/// Also handles hash-based expected results (e.g., "30 values hashing to abc123...").
fn auto_detect_validator(
    normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    // Handle empty results
    if actual.is_empty() {
        return expected.is_empty() || (expected.len() == 1 && expected[0].trim().is_empty());
    }
    
    // Check if expected result is hash-based (e.g., "30 values hashing to abc123...")
    if expected.len() == 1 {
        let hash_regex = Regex::new(r"^(\d+) values hashing to ([a-f0-9]+)$").unwrap();
        if let Some(caps) = hash_regex.captures(expected[0].trim()) {
            let expected_count: usize = caps[1].parse().unwrap();
            let expected_hash = &caps[2];
            
            // Calculate total number of values
            let total_actual_values: usize = actual.iter().map(|row| row.len()).sum();
            
            if expected_count != total_actual_values {
                return false;
            }
            
            // Compute hash of actual results (flattened, one value per line)
            // Build the string to hash (one value per line, normalized)
            let mut hash_input = Vec::new();
            for row in actual {
                for value in row {
                    let normalized = normalizer(value);
                    hash_input.extend_from_slice(normalized.as_bytes());
                    hash_input.push(b'\n');
                }
            }
            let hash_bytes = md5::compute(&hash_input);
            let actual_hash = format!("{:x}", hash_bytes);
            
            return actual_hash == *expected_hash;
        }
    }
    
    // Calculate total number of values in actual results
    let total_actual_values: usize = actual.iter().map(|row| row.len()).sum();
    let num_columns = actual[0].len();
    
    // If expected has same number of lines as total values AND there are multiple columns,
    // it's valuewise format. For single-column queries, both formats are equivalent.
    let is_valuewise = expected.len() == total_actual_values && num_columns > 1;
    
    if is_valuewise {
        // Flatten actual results into individual values (valuewise format)
        let flattened_actual: Vec<String> = actual
            .iter()
            .flat_map(|row| row.iter())
            .map(|s| normalizer(s))
            .collect();
        
        let normalized_expected: Vec<String> = expected
            .iter()
            .map(normalizer)
            .collect();
        
        flattened_actual == normalized_expected
    } else {
        // Use default rowwise format: join columns with spaces
        let normalized_rows: Vec<String> = actual
            .iter()
            .map(|row| row.iter().map(normalizer).join(" "))
            .collect();
        
        let normalized_expected: Vec<String> = expected
            .iter()
            .map(normalizer)
            .collect();
        
        normalized_rows == normalized_expected
    }
}

/// Helper function to run a single test file
pub async fn run_test_file(test_file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut tester = sqllogictest::Runner::new(|| async {
        let db = SQLiteDB::new()?;
        Ok::<_, rusqlite::Error>(db)
    });
    // Set custom validator that auto-detects valuewise vs rowwise format
    tester.with_validator(auto_detect_validator);
    tester.run_file(test_file)?;
    Ok(())
}
