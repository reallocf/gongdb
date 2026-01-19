use gongdb::run_test_file;
use std::io::{self, Write};
use tokio::time::{timeout, Duration};

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

fn parse_args() -> (Option<u64>, Vec<String>, Vec<String>) {
    let mut args = std::env::args().skip(1);
    let mut timeout_secs = None;
    let mut skip_substrs = Vec::new();
    let mut only_substrs = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--timeout-secs" => {
                if let Some(value) = args.next() {
                    if let Ok(parsed) = value.parse::<u64>() {
                        timeout_secs = Some(parsed);
                    }
                }
            }
            "--skip-substr" => {
                if let Some(value) = args.next() {
                    skip_substrs.push(value);
                }
            }
            "--only-substr" => {
                if let Some(value) = args.next() {
                    only_substrs.push(value);
                }
            }
            _ => {}
        }
    }

    (timeout_secs, skip_substrs, only_substrs)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (timeout_secs, skip_substrs, only_substrs) = parse_args();
    // Discover and run all test files (SQLite + custom)
    let mut test_files = Vec::new();
    
    // Collect SQLite test files
    collect_test_files(std::path::Path::new("tests/sqlite"), &mut test_files)?;
    
    // Collect custom test files
    collect_test_files(std::path::Path::new("tests/custom"), &mut test_files)?;
    
    test_files.sort();

    if !skip_substrs.is_empty() {
        test_files.retain(|path| !skip_substrs.iter().any(|skip| path.contains(skip)));
    }
    if !only_substrs.is_empty() {
        test_files.retain(|path| only_substrs.iter().any(|only| path.contains(only)));
    }
    
    println!("Found {} test files (SQLite + custom)", test_files.len());
    io::stdout().flush()?;
    
    let mut failed = Vec::new();
    let mut timed_out = Vec::new();
    for test_file in &test_files {
        print!("Running {}... ", test_file);
        io::stdout().flush()?;
        let run = run_test_file(test_file);
        let result = if let Some(secs) = timeout_secs {
            match timeout(Duration::from_secs(secs), run).await {
                Ok(inner) => inner,
                Err(_) => {
                    println!("TIMEOUT");
                    io::stdout().flush()?;
                    timed_out.push(test_file.clone());
                    continue;
                }
            }
        } else {
            run.await
        };

        match result {
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
    
    if failed.is_empty() && timed_out.is_empty() {
        println!("\nAll {} tests passed!", test_files.len());
        io::stdout().flush()?;
        Ok(())
    } else {
        if !failed.is_empty() {
            eprintln!("\n{} out of {} tests failed:", failed.len(), test_files.len());
            io::stderr().flush()?;
            for (file, _) in &failed {
                eprintln!("  - {}", file);
                io::stderr().flush()?;
            }
        }
        if !timed_out.is_empty() {
            eprintln!("\n{} out of {} tests timed out:", timed_out.len(), test_files.len());
            io::stderr().flush()?;
            for file in &timed_out {
                eprintln!("  - {}", file);
                io::stderr().flush()?;
            }
        }
        std::process::exit(1);
    }
}
