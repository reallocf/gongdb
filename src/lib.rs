//! Core library for GongDB, a from-scratch SQLite-like engine.
//!
//! The public API is intentionally small: the parser, AST, storage layer, and
//! execution engine are exposed for experimentation and testing.
//!
//! # Examples
//! ```no_run
//! use gongdb::engine::GongDB;
//!
//! let mut db = GongDB::new_in_memory().expect("db");
//! db.run_statement("CREATE TABLE t(id INTEGER)")
//!     .expect("create");
//! db.run_statement("INSERT INTO t VALUES (1)")
//!     .expect("insert");
//! let output = db.run_statement("SELECT id FROM t").expect("select");
//! println!("{:?}", output);
//! ```

use sqllogictest::{DefaultColumnType, Normalizer};
use regex::Regex;
use md5::{Digest, Md5};
use std::cell::RefCell;
use std::fs;

/// Abstract syntax tree types for parsed SQL.
pub mod ast;
/// SQL parser entrypoints and error types.
pub mod parser;
/// Storage engine types, metadata, and errors.
pub mod storage;
/// Execution engine for running SQL statements.
pub mod engine;

thread_local! {
    pub(crate) static EXPECTED_TYPES: RefCell<Option<Vec<DefaultColumnType>>> = RefCell::new(None);
    static CURRENT_SORT_MODE: RefCell<Option<sqllogictest::parser::SortMode>> = RefCell::new(None);
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
    let sort_mode = CURRENT_SORT_MODE.with(|mode| mode.borrow().clone());
    fn normalize_integer(value: &str) -> String {
        let trimmed = value.trim();
        if trimmed.eq_ignore_ascii_case("NULL") {
            return "NULL".to_string();
        }
        if let Ok(int_val) = trimmed.parse::<i64>() {
            return int_val.to_string();
        }
        if let Ok(float_val) = trimmed.parse::<f64>() {
            if float_val.is_finite() {
                return (float_val.trunc() as i64).to_string();
            }
        }
        "0".to_string()
    }

    fn normalize_float(value: &str) -> String {
        let trimmed = value.trim();
        if trimmed.eq_ignore_ascii_case("NULL") {
            return "NULL".to_string();
        }
        if let Ok(float_val) = trimmed.parse::<f64>() {
            if float_val.is_finite() {
                return float_val.to_string();
            }
        }
        "0.0".to_string()
    }

    fn normalize_by_type(
        normalizer: Normalizer,
        value: &str,
        column_type: Option<&DefaultColumnType>,
    ) -> String {
        match column_type {
            Some(DefaultColumnType::Integer) => normalize_integer(value),
            Some(DefaultColumnType::FloatingPoint) => normalize_float(value),
            Some(DefaultColumnType::Text) | Some(DefaultColumnType::Any) | None => {
                normalizer(&value.to_string())
            }
        }
    }

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
            if actual_norm.eq_ignore_ascii_case("NULL") {
                return false;
            }
            if let Ok(actual_float) = actual_norm.parse::<f64>() {
                if actual_float.is_finite() {
                    return expected_int == actual_float.trunc() as i64;
                }
            }
            return expected_int == 0;
        }
        false
    }

    fn hash_rows(
        rows: &[Vec<String>],
    ) -> String {
        let mut md5 = Md5::new();
        for row in rows {
            for value in row {
                md5.update(value.as_bytes());
                md5.update(b"\n");
            }
        }
        format!("{:2x}", md5.finalize())
    }

    fn normalized_rows_for_hash(
        normalizer: Normalizer,
        actual: &[Vec<String>],
        expected_types: Option<&[DefaultColumnType]>,
    ) -> Vec<Vec<String>> {
        let types = expected_types.unwrap_or(&[]);
        let mut index = 0usize;
        let mut rows = Vec::with_capacity(actual.len());
        for row in actual {
            let mut normalized_row = Vec::with_capacity(row.len());
            for value in row {
                let column_type = if types.is_empty() {
                    None
                } else {
                    Some(&types[index % types.len()])
                };
                normalized_row.push(normalize_by_type(normalizer, value, column_type));
                index += 1;
            }
            rows.push(normalized_row);
        }
        rows
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
            
            let expected_types = EXPECTED_TYPES.with(|types| types.borrow().clone());
            let mut normalized_rows =
                normalized_rows_for_hash(normalizer, actual, expected_types.as_deref());

            let total_actual_values: usize =
                normalized_rows.iter().map(|row| row.len()).sum();
            if expected_count != total_actual_values {
                return false;
            }

            normalized_rows.sort_unstable();
            let normalized_hash = hash_rows(&normalized_rows);
            if normalized_hash == *expected_hash {
                return true;
            }

            let raw_hash = hash_rows(actual);
            if raw_hash == *expected_hash {
                return true;
            }
            return false;
        }
    }
    
    // Calculate total number of values in actual results
    let total_actual_values: usize = actual.iter().map(|row| row.len()).sum();
    let num_columns = actual[0].len();

    // If expected has same number of lines as total values AND there are multiple columns,
    // it's valuewise format. For single-column queries, both formats are equivalent.
    let is_valuewise = expected.len() == total_actual_values && num_columns > 1;

    if matches!(
        sort_mode,
        Some(sqllogictest::parser::SortMode::RowSort)
            | Some(sqllogictest::parser::SortMode::ValueSort)
    ) {
        let expected_types = EXPECTED_TYPES.with(|types| types.borrow().clone());
        let expected_rows: Vec<Vec<String>> = if is_valuewise
            || matches!(
                sort_mode,
                Some(sqllogictest::parser::SortMode::ValueSort)
            ) {
            expected.iter().map(|line| vec![line.clone()]).collect()
        } else {
            expected
                .iter()
                .map(|line| line.split_ascii_whitespace().map(|s| s.to_string()).collect())
                .collect()
        };
        let actual_rows: Vec<Vec<String>> = if is_valuewise
            || matches!(
                sort_mode,
                Some(sqllogictest::parser::SortMode::ValueSort)
            ) {
            actual
                .iter()
                .flat_map(|row| row.iter().cloned())
                .map(|value| vec![value])
                .collect()
        } else {
            actual.to_vec()
        };
        let mut normalized_actual =
            normalized_rows_for_hash(normalizer, &actual_rows, expected_types.as_deref());
        let mut normalized_expected =
            normalized_rows_for_hash(normalizer, &expected_rows, expected_types.as_deref());
        normalized_actual.sort_unstable();
        normalized_expected.sort_unstable();
        return normalized_actual == normalized_expected;
    }

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
    let mut tester = sqllogictest::Runner::new(|| async {
        let db = engine::GongDB::new_in_memory()?;
        Ok::<_, engine::GongDBError>(db)
    });
    // Set custom validator that auto-detects valuewise vs rowwise format
    tester.with_validator(auto_detect_validator);
    tester.with_column_validator(|_, expected| {
        EXPECTED_TYPES.with(|types| {
            *types.borrow_mut() = Some(expected.clone());
        });
        true
    });
    // Add "sqlite" label so skipif/onlyif directives work correctly
    tester.add_label("sqlite");

    // Preprocess the test file to strip comments from skipif/onlyif lines
    let preprocessed = preprocess_test_file(test_file)?;

    // Parse and run the preprocessed content
    // Use the parser module directly since parse_with_name is not exported
    let records =
        sqllogictest::parser::parse_with_name::<DefaultColumnType>(&preprocessed, test_file)?;
    let mut current_sort_mode: Option<sqllogictest::parser::SortMode> = None;
    for record in records {
        if matches!(record, sqllogictest::parser::Record::Halt { .. }) {
            break;
        }
        match &record {
            sqllogictest::parser::Record::Control(
                sqllogictest::parser::Control::SortMode(mode),
            ) => {
                current_sort_mode = Some(mode.clone());
                EXPECTED_TYPES.with(|stored| {
                    *stored.borrow_mut() = None;
                });
            }
            sqllogictest::parser::Record::Query { expected, .. } => match expected {
                sqllogictest::parser::QueryExpect::Results { types, sort_mode, .. } => {
                    EXPECTED_TYPES.with(|stored| {
                        *stored.borrow_mut() = Some(types.clone());
                    });
                    if sort_mode.is_some() {
                        current_sort_mode = sort_mode.clone();
                    }
                }
                sqllogictest::parser::QueryExpect::Error(_) => {
                    EXPECTED_TYPES.with(|stored| {
                        *stored.borrow_mut() = None;
                    });
                }
            },
            _ => {
                EXPECTED_TYPES.with(|stored| {
                    *stored.borrow_mut() = None;
                });
            }
        }
        CURRENT_SORT_MODE.with(|stored| {
            *stored.borrow_mut() = current_sort_mode.clone();
        });
        tester.run(record)?;
    }
    Ok(())
}
