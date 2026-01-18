use sqllogictest::{DBOutput, DefaultColumnType, Normalizer};
use rusqlite::Connection;
use async_trait::async_trait;
use regex::Regex;
use md5::{Digest, Md5};
use std::fs;

pub mod ast;
pub mod parser;
pub mod storage;
pub mod engine;

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
                    use rusqlite::types::ValueRef;
                    let value = match row.get_ref(i).unwrap() {
                        ValueRef::Null => "NULL".to_string(),
                        ValueRef::Integer(i) => i.to_string(),
                        ValueRef::Real(f) => (f.trunc() as i64).to_string(),
                        ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                        ValueRef::Blob(_) => "NULL".to_string(),
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
    fn values_match(normalizer: Normalizer, actual: &str, expected: &str) -> bool {
        let actual_norm = normalizer(&actual.to_string());
        let expected_norm = normalizer(&expected.to_string());
        if actual_norm == expected_norm {
            return true;
        }
        if expected_norm.eq_ignore_ascii_case("NULL") {
            return actual_norm.eq_ignore_ascii_case("NULL");
        }
        if let Ok(expected_int) = expected_norm.parse::<i64>() {
            if let Ok(actual_float) = actual_norm.parse::<f64>() {
                if actual_float.is_finite() {
                    return expected_int == actual_float.trunc() as i64;
                }
            }
        }
        false
    }

    fn normalize_for_hash(normalizer: Normalizer, value: &str) -> String {
        let normalized = normalizer(&value.to_string());
        if let Ok(int_val) = normalized.parse::<i64>() {
            return int_val.to_string();
        }
        if let Ok(float_val) = normalized.parse::<f64>() {
            if float_val.is_finite() {
                return (float_val.trunc() as i64).to_string();
            }
        }
        normalized
    }

    // Handle empty results
    if actual.is_empty() {
        return expected.is_empty() || (expected.len() == 1 && expected[0].trim().is_empty());
    }
    
    // Check if expected result is hash-based (e.g., "30 values hashing to abc123...")
    // IMPORTANT: When hash_threshold is exceeded, the library computes the hash and replaces
    // the actual rows with a hash string BEFORE passing to the validator.
    // So `actual` will be the hash string, not the original rows.
    if expected.len() == 1 {
        let hash_regex = Regex::new(r"^(\d+) values hashing to ([a-f0-9]+)$").unwrap();
        if let Some(expected_caps) = hash_regex.captures(expected[0].trim()) {
            // Check if actual is also a hash string (library already computed it)
            if actual.len() == 1 && actual[0].len() == 1 {
                if let Some(actual_caps) = hash_regex.captures(actual[0][0].trim()) {
                    // Both are hash strings - just compare the hashes
                    let expected_hash = &expected_caps[2];
                    let actual_hash = &actual_caps[2];
                    return actual_hash == expected_hash;
                }
            }
            // If actual is not a hash string, the library didn't replace it (threshold not exceeded?)
            // This shouldn't happen, but handle it by computing hash ourselves
            let expected_count: usize = expected_caps[1].parse().unwrap();
            let expected_hash = &expected_caps[2];
            
            // Calculate total number of values
            let total_actual_values: usize = actual.iter().map(|row| row.len()).sum();
            
            if expected_count != total_actual_values {
                return false;
            }
            
            // Compute hash of actual results (flattened, one value per line)
            // Use the exact same API as sqllogictest: md5::Md5::new() with update() and finalize()
            let mut md5 = Md5::new();
            for row in actual {
                for value in row {
                    let normalized = normalize_for_hash(normalizer, value);
                    md5.update(normalized.as_bytes());
                    md5.update(b"\n");
                }
            }
            // Format exactly as library does: {:2x} on the digest
            let actual_hash = format!("{:2x}", md5.finalize());
            
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
        let flattened_actual: Vec<String> = actual
            .iter()
            .flat_map(|row| row.iter().cloned())
            .collect();
        return flattened_actual
            .iter()
            .zip(expected.iter())
            .all(|(actual_val, expected_val)| values_match(normalizer, actual_val, expected_val));
    }

    if actual.len() != expected.len() {
        return false;
    }

    for (row, expected_line) in actual.iter().zip(expected.iter()) {
        let joined_actual = row.join(" ");
        if values_match(normalizer, &joined_actual, expected_line) {
            continue;
        }
        let expected_parts: Vec<&str> = expected_line.split_ascii_whitespace().collect();
        if expected_parts.len() != row.len() {
            return false;
        }
        if !row
            .iter()
            .zip(expected_parts.iter())
            .all(|(actual_val, expected_val)| values_match(normalizer, actual_val, expected_val))
        {
            return false;
        }
    }

    true
}

/// Preprocess test file to strip comments from skipif/onlyif lines
/// The sqllogictest parser doesn't support comments after these directives
fn preprocess_test_file(test_file: &str) -> Result<String, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(test_file)?;
    let mut lines = Vec::new();
    
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("hash-threshold") {
            lines.push("hash-threshold 0".to_string());
            continue;
        }
        // Strip comments from skipif/onlyif lines
        if trimmed.starts_with("skipif ") || trimmed.starts_with("onlyif ") {
            // Find the first '#' that's not part of the directive itself
            // The directive format is "skipif label" or "onlyif label"
            // We want to keep everything up to (but not including) the first '#'
            if let Some(comment_pos) = line.find(" #") {
                // Only strip if there's a space before the # (to avoid stripping # in the label itself)
                lines.push(line[..comment_pos].to_string());
            } else {
                lines.push(line.to_string());
            }
        } else {
            lines.push(line.to_string());
        }
    }
    
    Ok(lines.join("\n"))
}

/// Helper function to run a single test file
pub async fn run_test_file(test_file: &str) -> Result<(), Box<dyn std::error::Error>> {
    if test_file.ends_with("phase2_storage_engine.test") {
        let mut tester = sqllogictest::Runner::new(|| async {
            let db = engine::GongDB::new_in_memory()?;
            Ok::<_, engine::GongDBError>(db)
        });
        tester.with_validator(auto_detect_validator);
        let preprocessed = preprocess_test_file(test_file)?;
        let records =
            sqllogictest::parser::parse_with_name::<DefaultColumnType>(&preprocessed, test_file)?;
        tester.run_multi(records)?;
        return Ok(());
    }

    let mut tester = sqllogictest::Runner::new(|| async {
        let db = SQLiteDB::new()?;
        Ok::<_, rusqlite::Error>(db)
    });
    // Set custom validator that auto-detects valuewise vs rowwise format
    tester.with_validator(auto_detect_validator);
    // Add "sqlite" label so skipif/onlyif directives work correctly
    tester.add_label("sqlite");

    // Preprocess the test file to strip comments from skipif/onlyif lines
    let preprocessed = preprocess_test_file(test_file)?;

    // Parse and run the preprocessed content
    // Use the parser module directly since parse_with_name is not exported
    let records =
        sqllogictest::parser::parse_with_name::<DefaultColumnType>(&preprocessed, test_file)?;
    tester.run_multi(records)?;
    Ok(())
}
