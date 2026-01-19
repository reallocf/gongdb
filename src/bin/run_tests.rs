use gongdb::run_test_file;
use std::io::{self, Write};

/// Helper function to collect all test files recursively
fn collect_test_files(dir: &std::path::Path, test_files: &mut Vec<String>) -> std::io::Result<()> {
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Discover and run all test files (SQLite + custom)
    let mut test_files = Vec::new();
    
    // Collect SQLite test files
    collect_test_files(std::path::Path::new("tests/sqlite"), &mut test_files)?;
    
    // Collect custom test files
    collect_test_files(std::path::Path::new("tests/custom"), &mut test_files)?;
    
    test_files.sort();
    
    println!("Found {} test files (SQLite + custom)", test_files.len());
    io::stdout().flush()?;
    
    let mut failed = Vec::new();
    for test_file in &test_files {
        print!("Running {}... ", test_file);
        io::stdout().flush()?;
        match run_test_file(test_file).await {
            Ok(()) => {
                println!("✓");
                io::stdout().flush()?;
            }
            Err(e) => {
                println!("✗");
                io::stdout().flush()?;
                eprintln!("  Error: {}", e);
                io::stderr().flush()?;
                failed.push((test_file.clone(), e));
            }
        }
    }
    
    if failed.is_empty() {
        println!("\nAll {} tests passed!", test_files.len());
        io::stdout().flush()?;
        Ok(())
    } else {
        eprintln!("\n{} out of {} tests failed:", failed.len(), test_files.len());
        io::stderr().flush()?;
        for (file, _) in &failed {
            eprintln!("  - {}", file);
            io::stderr().flush()?;
        }
        std::process::exit(1);
    }
}
