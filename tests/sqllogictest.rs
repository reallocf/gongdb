use gongdb::run_test_file;
use std::fs;
use std::path::Path;
use std::io::{self, Write};

/// Helper function to collect all test files recursively
fn collect_test_files(dir: &Path, test_files: &mut Vec<String>) -> std::io::Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                collect_test_files(&path, test_files)?;
            } else if path.extension().and_then(|s| s.to_str()) == Some("test") {
                if let Some(path_str) = path.to_str() {
                    test_files.push(path_str.to_string());
                }
            }
        }
    }
    Ok(())
}

/// Helper to run a single test file by path
/// This allows running individual test files: cargo test -- test_select1
macro_rules! test_file {
    ($name:ident, $path:expr) => {
        #[tokio::test]
        async fn $name() {
            run_test_file($path).await
                .unwrap_or_else(|e| panic!("Test {} failed: {}", $path, e));
        }
    };
}

/// Helper to run multiple test files for a phase
async fn run_phase_tests(test_files: &[&str], phase_name: &str) {
    let mut failed = Vec::new();
    let total = test_files.len();
    
    println!("\n=== Phase {} Tests ===", phase_name);
    io::stdout().flush().unwrap();
    
    for (idx, test_file) in test_files.iter().enumerate() {
        print!("[{}/{}] Running {}... ", idx + 1, total, test_file);
        io::stdout().flush().unwrap();
        
        match run_test_file(test_file).await {
            Ok(()) => {
                println!("OK");
                io::stdout().flush().unwrap();
            }
            Err(e) => {
                println!("FAILED");
                io::stdout().flush().unwrap();
                eprintln!("  Error: {}", e);
                io::stderr().flush().unwrap();
                failed.push((test_file.to_string(), e));
            }
        }
    }
    
    if !failed.is_empty() {
        eprintln!("\n{} out of {} Phase {} tests failed:", failed.len(), total, phase_name);
        io::stderr().flush().unwrap();
        for (file, _) in &failed {
            eprintln!("  - {}", file);
            io::stderr().flush().unwrap();
        }
        panic!(
            "Phase {} test suite failed: {} out of {} tests failed",
            phase_name, failed.len(), total
        );
    }
    
    println!("All {} Phase {} tests passed!\n", total, phase_name);
    io::stdout().flush().unwrap();
}

// Individual test files for main test suite
// These can be run individually: cargo test test_select1
test_file!(test_select1, "tests/sqlite/select1.test");
test_file!(test_select2, "tests/sqlite/select2.test");
test_file!(test_select3, "tests/sqlite/select3.test");
test_file!(test_select4, "tests/sqlite/select4.test");
test_file!(test_select5, "tests/sqlite/select5.test");
test_file!(test_in1, "tests/sqlite/evidence/in1.test");
test_file!(test_in2, "tests/sqlite/evidence/in2.test");
test_file!(test_slt_lang_reindex, "tests/sqlite/evidence/slt_lang_reindex.test");

// Custom tests for phases that need additional coverage
test_file!(test_phase2_storage_engine, "tests/custom/phase2_storage_engine.test");
test_file!(test_phase3_type_system, "tests/custom/phase3_type_system.test");
test_file!(test_phase11_indexing, "tests/custom/phase11_indexing.test");
test_file!(test_phase12_query_planning, "tests/custom/phase12_query_planning.test");
test_file!(test_phase13_transactions, "tests/custom/phase13_transactions.test");

// Phase-specific test suites
// Run with: cargo test --test sqllogictest test_phase_1

/// Phase 1: Core Infrastructure - SQL Parser and AST
/// Tests basic parsing from select1.test
#[tokio::test]
async fn test_phase_1() {
    run_phase_tests(&[
        "tests/sqlite/select1.test",  // Basic CREATE, INSERT, SELECT parsing
    ], "1 (Parser/AST)").await;
}

/// Phase 2: Storage Engine
/// Tests page-based storage, persistence, and row serialization
#[tokio::test]
async fn test_phase_2() {
    run_phase_tests(&[
        "tests/custom/phase2_storage_engine.test",
        "tests/sqlite/select1.test",  // Verify data persistence
    ], "2 (Storage Engine)").await;
}

/// Phase 3: Type System
/// Tests type coercion, affinity, and NULL handling
#[tokio::test]
async fn test_phase_3() {
    run_phase_tests(&[
        "tests/custom/phase3_type_system.test",
        "tests/sqlite/select1.test",  // Mixed data types
    ], "3 (Type System)").await;
}

/// Phase 4: DDL Implementation
/// Tests CREATE/DROP TABLE, INDEX, VIEW
#[tokio::test]
async fn test_phase_4() {
    let mut test_files = Vec::new();
    collect_test_files(Path::new("tests/sqlite/evidence"), &mut test_files)
        .expect("Failed to collect evidence test files");
    
    // Filter to DDL-related tests
    let ddl_tests: Vec<String> = test_files.into_iter()
        .filter(|f| f.contains("createview") || f.contains("droptable") || 
                    f.contains("dropindex") || f.contains("dropview"))
        .collect();
    
    let test_files: Vec<&str> = ddl_tests.iter().map(|s| s.as_str()).collect();
    run_phase_tests(&test_files, "4 (DDL)").await;
}

/// Phase 5: Expression Evaluation
/// Tests arithmetic, logical operators, functions, CASE, IN, BETWEEN, EXISTS
#[tokio::test]
async fn test_phase_5() {
    let mut test_files = Vec::new();
    
    // Collect expression-related tests
    collect_test_files(Path::new("tests/sqlite/random/expr"), &mut test_files)
        .expect("Failed to collect expr test files");
    collect_test_files(Path::new("tests/sqlite/evidence"), &mut test_files)
        .expect("Failed to collect evidence test files");
    collect_test_files(Path::new("tests/sqlite/index/between"), &mut test_files)
        .expect("Failed to collect between test files");
    
    // Filter to expression-related tests
    let expr_tests: Vec<String> = test_files.into_iter()
        .filter(|f| f.contains("expr") || f.contains("in1") || f.contains("in2") || 
                    f.contains("between"))
        .collect();
    
    // Take first 10 expr tests for speed (can run all with --ignored)
    let test_files: Vec<&str> = expr_tests.iter().take(10).map(|s| s.as_str()).collect();
    run_phase_tests(&test_files, "5 (Expressions)").await;
}

/// Phase 5: Expression Evaluation (All Tests)
/// Run all expression tests - use with --ignored flag
#[tokio::test]
#[ignore]
async fn test_phase_5_all() {
    let mut test_files = Vec::new();
    
    collect_test_files(Path::new("tests/sqlite/random/expr"), &mut test_files)
        .expect("Failed to collect expr test files");
    collect_test_files(Path::new("tests/sqlite/evidence"), &mut test_files)
        .expect("Failed to collect evidence test files");
    collect_test_files(Path::new("tests/sqlite/index/between"), &mut test_files)
        .expect("Failed to collect between test files");
    
    let expr_tests: Vec<String> = test_files.into_iter()
        .filter(|f| f.contains("expr") || f.contains("in1") || f.contains("in2") || 
                    f.contains("between"))
        .collect();
    
    let test_files: Vec<&str> = expr_tests.iter().map(|s| s.as_str()).collect();
    run_phase_tests(&test_files, "5 (Expressions - All)").await;
}

/// Index IN test suite
#[tokio::test]
async fn test_index_in() {
    let mut test_files = Vec::new();
    collect_test_files(Path::new("tests/sqlite/index/in"), &mut test_files)
        .expect("Failed to collect index/in test files");
    test_files.sort();
    let test_files: Vec<&str> = test_files.iter().map(|s| s.as_str()).collect();
    run_phase_tests(&test_files, "Index IN").await;
}

/// ORDER BY test suites
#[tokio::test]
async fn test_orderby_suites() {
    let mut test_files = Vec::new();
    collect_test_files(Path::new("tests/sqlite/index/orderby"), &mut test_files)
        .expect("Failed to collect index/orderby test files");
    collect_test_files(Path::new("tests/sqlite/index/orderby_nosort"), &mut test_files)
        .expect("Failed to collect index/orderby_nosort test files");
    test_files.sort();
    let test_files: Vec<&str> = test_files.iter().map(|s| s.as_str()).collect();
    run_phase_tests(&test_files, "ORDER BY").await;
}

/// Phase 6: DML Implementation
/// Tests INSERT, UPDATE, DELETE, REPLACE
#[tokio::test]
async fn test_phase_6() {
    let mut test_files = Vec::new();
    
    collect_test_files(Path::new("tests/sqlite/evidence"), &mut test_files)
        .expect("Failed to collect evidence test files");
    collect_test_files(Path::new("tests/sqlite/index/delete"), &mut test_files)
        .expect("Failed to collect delete test files");
    
    // Filter to DML-related tests
    let dml_tests: Vec<String> = test_files.into_iter()
        .filter(|f| f.contains("update") || f.contains("replace") || f.contains("delete"))
        .collect();
    
    // Take first 5 delete tests for speed
    let mut test_files: Vec<&str> = dml_tests.iter().take(5).map(|s| s.as_str()).collect();
    test_files.push("tests/sqlite/select1.test");  // INSERT tests
    
    run_phase_tests(&test_files, "6 (DML)").await;
}

/// Phase 7: Query Execution - WHERE Clause Filtering
/// Tests select1 and random/select suites
#[tokio::test]
async fn test_phase_7() {
    let mut test_files = Vec::new();
    collect_test_files(Path::new("tests/sqlite/random/select"), &mut test_files)
        .expect("Failed to collect random/select test files");
    test_files.sort();
    let mut phase_tests = Vec::new();
    phase_tests.push("tests/sqlite/select1.test".to_string());
    phase_tests.extend(test_files);
    let test_files: Vec<&str> = phase_tests.iter().map(|s| s.as_str()).collect();
    run_phase_tests(&test_files, "7 (WHERE Clause Filtering)").await;
}

/// Phase 8: Aggregation
/// Tests GROUP BY and aggregate functions
#[tokio::test]
async fn test_phase_8() {
    let mut test_files = Vec::new();
    
    collect_test_files(Path::new("tests/sqlite/random/aggregates"), &mut test_files)
        .expect("Failed to collect aggregates test files");
    collect_test_files(Path::new("tests/sqlite/random/groupby"), &mut test_files)
        .expect("Failed to collect groupby test files");
    collect_test_files(Path::new("tests/sqlite/evidence"), &mut test_files)
        .expect("Failed to collect evidence test files");
    
    // Filter to aggregation-related tests
    let agg_tests: Vec<String> = test_files.into_iter()
        .filter(|f| f.contains("aggregates") || f.contains("groupby") || f.contains("aggfunc"))
        .collect();
    
    // Take first 5 for speed
    let test_files: Vec<&str> = agg_tests.iter().take(5).map(|s| s.as_str()).collect();
    run_phase_tests(&test_files, "8 (Aggregation)").await;
}

/// Phase 9: Joins
/// Tests INNER, LEFT, RIGHT, OUTER joins
#[tokio::test]
async fn test_phase_9() {
    run_phase_tests(&[
        "tests/sqlite/select2.test",
        "tests/sqlite/select3.test",
        "tests/sqlite/select4.test",
        "tests/sqlite/select5.test",
    ], "9 (Joins)").await;
}

/// Phase 10: Subqueries
/// Tests scalar subqueries, EXISTS, IN subqueries, derived tables, CTEs
#[tokio::test]
async fn test_phase_10() {
    let mut test_files = Vec::new();
    
    collect_test_files(Path::new("tests/sqlite/evidence"), &mut test_files)
        .expect("Failed to collect evidence test files");
    
    // Filter to subquery-related tests
    let subq_tests: Vec<String> = test_files.into_iter()
        .filter(|f| f.contains("in1") || f.contains("in2"))
        .collect();
    
    let mut test_files: Vec<&str> = subq_tests.iter().map(|s| s.as_str()).collect();
    test_files.extend(&[
        "tests/sqlite/select1.test",  // Has many subqueries
        "tests/sqlite/select2.test",
    ]);
    
    run_phase_tests(&test_files, "10 (Subqueries)").await;
}

/// Phase 11: Indexing
/// Tests B-tree structure, index scan, index maintenance
#[tokio::test]
async fn test_phase_11() {
    let mut test_files = Vec::new();
    
    test_files.push("tests/custom/phase11_indexing.test".to_string());
    
    collect_test_files(Path::new("tests/sqlite/index"), &mut test_files)
        .expect("Failed to collect index test files");
    collect_test_files(Path::new("tests/sqlite/evidence"), &mut test_files)
        .expect("Failed to collect evidence test files");
    
    // Filter to indexing-related tests
    let index_tests: Vec<String> = test_files.into_iter()
        .filter(|f| f.contains("index") || f.contains("dropindex") || f.contains("reindex"))
        .collect();
    
    // Take first 10 for speed
    let test_files: Vec<&str> = index_tests.iter().take(10).map(|s| s.as_str()).collect();
    run_phase_tests(&test_files, "11 (Indexing)").await;
}

/// Phase 12: Query Planning
/// Tests query planner, cost estimation, optimization
#[tokio::test]
async fn test_phase_12() {
    run_phase_tests(&[
        "tests/custom/phase12_query_planning.test",
        "tests/sqlite/select1.test",
        "tests/sqlite/select2.test",
    ], "12 (Query Planning)").await;
}

/// Phase 13: Transaction Management
/// Tests ACID properties, transactions, locking, concurrency
#[tokio::test]
async fn test_phase_13() {
    run_phase_tests(&[
        "tests/custom/phase13_transactions.test",
    ], "13 (Transactions)").await;
}

/// Phase 14: Integration
/// Tests end-to-end integration with select1.test subset
#[tokio::test]
async fn test_phase_14() {
    run_phase_tests(&[
        "tests/sqlite/select1.test",
    ], "14 (Integration)").await;
}

/// Integration test that runs all custom test files
#[tokio::test]
async fn test_all_custom_files() {
    let mut test_files = Vec::new();
    collect_test_files(Path::new("tests/custom"), &mut test_files)
        .expect("Failed to collect custom test files");
    test_files.sort();
    
    let mut failed = Vec::new();
    let total = test_files.len();
    
    println!("Running {} custom test files...", total);
    io::stdout().flush().unwrap();
    
    for (idx, test_file) in test_files.iter().enumerate() {
        print!("[{}/{}] Running {}... ", idx + 1, total, test_file);
        io::stdout().flush().unwrap();
        
        match run_test_file(test_file).await {
            Ok(()) => {
                println!("OK");
                io::stdout().flush().unwrap();
            }
            Err(e) => {
                println!("FAILED");
                io::stdout().flush().unwrap();
                eprintln!("  Error: {}", e);
                io::stderr().flush().unwrap();
                failed.push((test_file.clone(), e));
            }
        }
    }
    
    if !failed.is_empty() {
        eprintln!("\n{} out of {} custom tests failed:", failed.len(), total);
        io::stderr().flush().unwrap();
        for (file, _) in &failed {
            eprintln!("  - {}", file);
            io::stderr().flush().unwrap();
        }
        panic!(
            "Custom test suite failed: {} out of {} tests failed",
            failed.len(),
            total
        );
    }
    
    println!("\nAll {} custom tests passed!", total);
    io::stdout().flush().unwrap();
}

/// Integration test that runs all SQLite test files
/// Each file is run as a separate logical test unit with better error reporting
#[tokio::test]
#[ignore] // Ignore by default since it takes a long time - run with: cargo test -- --ignored
async fn test_all_sqlite_files() {
    let mut test_files = Vec::new();
    collect_test_files(Path::new("tests/sqlite"), &mut test_files)
        .expect("Failed to collect test files");
    test_files.sort();
    
    let mut failed = Vec::new();
    let total = test_files.len();
    
    println!("Running {} test files...", total);
    io::stdout().flush().unwrap();
    
    for (idx, test_file) in test_files.iter().enumerate() {
        // Print progress for every test, flushing immediately
        print!("[{}/{}] Running {}... ", idx + 1, total, test_file);
        io::stdout().flush().unwrap();
        
        match run_test_file(test_file).await {
            Ok(()) => {
                println!("OK");
                io::stdout().flush().unwrap();
            }
            Err(e) => {
                println!("FAILED");
                io::stdout().flush().unwrap();
                eprintln!("  Error: {}", e);
                io::stderr().flush().unwrap();
                failed.push((test_file.clone(), e));
            }
        }
    }
    
    if !failed.is_empty() {
        eprintln!("\n{} out of {} tests failed:", failed.len(), total);
        io::stderr().flush().unwrap();
        for (file, _) in &failed {
            eprintln!("  - {}", file);
            io::stderr().flush().unwrap();
        }
        panic!(
            "Test suite failed: {} out of {} tests failed",
            failed.len(),
            total
        );
    }
    
    println!("\nAll {} tests passed!", total);
    io::stdout().flush().unwrap();
}
