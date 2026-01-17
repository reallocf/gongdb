use gongdb::run_test_file;
use std::fs;
use std::path::Path;

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

// Individual test files for main test suite
// These can be run individually: cargo test test_select1
test_file!(test_select1, "tests/sqlite/select1.test");
test_file!(test_select2, "tests/sqlite/select2.test");
test_file!(test_select3, "tests/sqlite/select3.test");
test_file!(test_select4, "tests/sqlite/select4.test");
test_file!(test_select5, "tests/sqlite/select5.test");

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
    
    for (idx, test_file) in test_files.iter().enumerate() {
        if idx % 50 == 0 {
            println!("Progress: {}/{}", idx, total);
        }
        match run_test_file(test_file).await {
            Ok(()) => {}
            Err(e) => {
                eprintln!("FAILED: {} - {}", test_file, e);
                failed.push((test_file.clone(), e));
            }
        }
    }
    
    if !failed.is_empty() {
        eprintln!("\n{} out of {} tests failed:", failed.len(), total);
        for (file, _) in &failed {
            eprintln!("  - {}", file);
        }
        panic!(
            "Test suite failed: {} out of {} tests failed",
            failed.len(),
            total
        );
    }
    
    println!("\nAll {} tests passed!", total);
}
