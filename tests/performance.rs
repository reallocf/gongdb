//! Performance tests comparing gongdb to rusqlite.
//!
//! These tests measure execution time for various operations and compare
//! gongdb's performance against rusqlite as a reference implementation.
//!
//! Run with: `cargo test --test performance -- --nocapture`

use gongdb::engine::GongDB;
use rusqlite::Connection;
use sqllogictest::DBOutput;
use std::time::Instant;

/// Helper to time a closure execution
fn time_it<F>(f: F) -> (f64, String)
where
    F: FnOnce() -> Result<String, Box<dyn std::error::Error>>,
{
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed();
    let result_str = result.unwrap_or_else(|e| format!("ERROR: {}", e));
    (elapsed.as_secs_f64(), result_str)
}

/// Helper to format timing results
fn format_result(name: &str, gongdb_time: f64, rusqlite_time: f64) {
    let ratio = if rusqlite_time > 0.0 {
        gongdb_time / rusqlite_time
    } else {
        f64::INFINITY
    };
    println!(
        "  {}: gongdb={:.3}s, rusqlite={:.3}s, ratio={:.2}x",
        name, gongdb_time, rusqlite_time, ratio
    );
}

/// Test: Bulk INSERT performance
#[test]
fn test_bulk_insert() {
    println!("\n=== Bulk INSERT Performance ===");
    
    let sizes = vec![100, 1000, 10000];
    
    for size in sizes {
        // Test gongdb
        let (gongdb_time, _) = time_it(|| {
            let mut db = GongDB::new_in_memory()?;
            db.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)")?;
            
            for i in 0..size {
                let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i * 2);
                db.run_statement(&sql)?;
            }
            Ok(format!("inserted {} rows", size))
        });
        
        // Test rusqlite
        let (rusqlite_time, _) = time_it(|| {
            let conn = Connection::open_in_memory()?;
            conn.execute("CREATE TABLE t1(id INTEGER, val INTEGER)", [])?;
            
            for i in 0..size {
                let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i * 2);
                conn.execute(&sql, [])?;
            }
            Ok(format!("inserted {} rows", size))
        });
        
        format_result(&format!("{} rows", size), gongdb_time, rusqlite_time);
    }
}

/// Test: SELECT with WHERE clause filtering
#[test]
fn test_select_where() {
    println!("\n=== SELECT with WHERE Performance ===");
    
    let size = 10000;
    
    // Setup: Insert data
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    gongdb.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        gongdb.run_statement(&sql).unwrap();
    }
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    conn.execute("CREATE TABLE t1(id INTEGER, val INTEGER)", []).unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        conn.execute(&sql, []).unwrap();
    }
    
    // Test: Select with WHERE
    let (gongdb_time, _) = time_it(|| {
        let mut count = 0;
        for _ in 0..100 {
            let result = gongdb.run_statement("SELECT * FROM t1 WHERE val = 50")?;
            if let DBOutput::Rows { rows, .. } = result {
                count += rows.len();
            }
        }
        Ok(format!("selected {} rows", count))
    });
    
    let (rusqlite_time, _) = time_it(|| {
        let mut count = 0;
        for _ in 0..100 {
            let mut stmt = conn.prepare("SELECT * FROM t1 WHERE val = 50")?;
            let rows = stmt.query_map([], |_| Ok(()))?;
            count += rows.count();
        }
        Ok(format!("selected {} rows", count))
    });
    
    format_result("WHERE filter (100 queries)", gongdb_time, rusqlite_time);
}

/// Test: Aggregation performance (COUNT, SUM, AVG)
#[test]
fn test_aggregation() {
    println!("\n=== Aggregation Performance ===");
    
    let size = 10000;
    
    // Setup
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    gongdb.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        gongdb.run_statement(&sql).unwrap();
    }
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    conn.execute("CREATE TABLE t1(id INTEGER, val INTEGER)", []).unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        conn.execute(&sql, []).unwrap();
    }
    
    // Test COUNT
    let (gongdb_time, _) = time_it(|| {
        for _ in 0..100 {
            gongdb.run_statement("SELECT COUNT(*) FROM t1")?;
        }
        Ok("count queries".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for _ in 0..100 {
            let mut stmt = conn.prepare("SELECT COUNT(*) FROM t1")?;
            stmt.query_row([], |_| Ok(()))?;
        }
        Ok("count queries".to_string())
    });
    
    format_result("COUNT(*) (100 queries)", gongdb_time, rusqlite_time);
    
    // Test SUM
    let (gongdb_time, _) = time_it(|| {
        for _ in 0..100 {
            gongdb.run_statement("SELECT SUM(val) FROM t1")?;
        }
        Ok("sum queries".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for _ in 0..100 {
            let mut stmt = conn.prepare("SELECT SUM(val) FROM t1")?;
            stmt.query_row([], |_| Ok(()))?;
        }
        Ok("sum queries".to_string())
    });
    
    format_result("SUM(val) (100 queries)", gongdb_time, rusqlite_time);
    
    // Test GROUP BY
    let (gongdb_time, _) = time_it(|| {
        for _ in 0..50 {
            gongdb.run_statement("SELECT val, COUNT(*) FROM t1 GROUP BY val")?;
        }
        Ok("group by queries".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for _ in 0..50 {
            let mut stmt = conn.prepare("SELECT val, COUNT(*) FROM t1 GROUP BY val")?;
            let rows = stmt.query_map([], |_| Ok(()))?;
            rows.count();
        }
        Ok("group by queries".to_string())
    });
    
    format_result("GROUP BY (50 queries)", gongdb_time, rusqlite_time);
}

/// Test: JOIN performance
#[test]
fn test_join() {
    println!("\n=== JOIN Performance ===");
    
    let size = 5000;
    
    // Setup: Create two tables with related data
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    gongdb.run_statement("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
    gongdb.run_statement("CREATE TABLE t2(id INTEGER, name TEXT)").unwrap();
    
    for i in 0..size {
        let sql1 = format!("INSERT INTO t1 VALUES ({}, {})", i, i * 2);
        let sql2 = format!("INSERT INTO t2 VALUES ({}, 'name{}')", i, i);
        gongdb.run_statement(&sql1).unwrap();
        gongdb.run_statement(&sql2).unwrap();
    }
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)", []).unwrap();
    conn.execute("CREATE TABLE t2(id INTEGER, name TEXT)", []).unwrap();
    
    for i in 0..size {
        let sql1 = format!("INSERT INTO t1 VALUES ({}, {})", i, i * 2);
        let sql2 = format!("INSERT INTO t2 VALUES ({}, 'name{}')", i, i);
        conn.execute(&sql1, []).unwrap();
        conn.execute(&sql2, []).unwrap();
    }
    
    // Test INNER JOIN
    let (gongdb_time, _) = time_it(|| {
        for _ in 0..50 {
            gongdb.run_statement("SELECT t1.val, t2.name FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.val < 1000")?;
        }
        Ok("join queries".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for _ in 0..50 {
            let mut stmt = conn.prepare("SELECT t1.val, t2.name FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.val < 1000")?;
            let rows = stmt.query_map([], |_| Ok(()))?;
            rows.count();
        }
        Ok("join queries".to_string())
    });
    
    format_result("INNER JOIN (50 queries)", gongdb_time, rusqlite_time);
}

/// Test: UPDATE performance
#[test]
fn test_update() {
    println!("\n=== UPDATE Performance ===");
    
    let size = 10000;
    
    // Setup
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    gongdb.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i);
        gongdb.run_statement(&sql).unwrap();
    }
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    conn.execute("CREATE TABLE t1(id INTEGER, val INTEGER)", []).unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i);
        conn.execute(&sql, []).unwrap();
    }
    
    // Test: Update with WHERE
    let (gongdb_time, _) = time_it(|| {
        gongdb.run_statement("UPDATE t1 SET val = val + 1 WHERE id < 5000")?;
        Ok("update query".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        conn.execute("UPDATE t1 SET val = val + 1 WHERE id < 5000", [])?;
        Ok("update query".to_string())
    });
    
    format_result("UPDATE with WHERE", gongdb_time, rusqlite_time);
}

/// Test: DELETE performance
#[test]
fn test_delete() {
    println!("\n=== DELETE Performance ===");
    
    let size = 10000;
    
    // Setup
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    gongdb.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i);
        gongdb.run_statement(&sql).unwrap();
    }
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    conn.execute("CREATE TABLE t1(id INTEGER, val INTEGER)", []).unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i);
        conn.execute(&sql, []).unwrap();
    }
    
    // Test: Delete with WHERE
    let (gongdb_time, _) = time_it(|| {
        gongdb.run_statement("DELETE FROM t1 WHERE id >= 5000")?;
        Ok("delete query".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        conn.execute("DELETE FROM t1 WHERE id >= 5000", [])?;
        Ok("delete query".to_string())
    });
    
    format_result("DELETE with WHERE", gongdb_time, rusqlite_time);
}

/// Test: ORDER BY performance
#[test]
fn test_order_by() {
    println!("\n=== ORDER BY Performance ===");
    
    let size = 10000;
    
    // Setup
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    gongdb.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, (i * 7) % 1000);
        gongdb.run_statement(&sql).unwrap();
    }
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    conn.execute("CREATE TABLE t1(id INTEGER, val INTEGER)", []).unwrap();
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, (i * 7) % 1000);
        conn.execute(&sql, []).unwrap();
    }
    
    // Test: ORDER BY
    let (gongdb_time, _) = time_it(|| {
        for _ in 0..20 {
            gongdb.run_statement("SELECT * FROM t1 ORDER BY val LIMIT 100")?;
        }
        Ok("order by queries".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for _ in 0..20 {
            let mut stmt = conn.prepare("SELECT * FROM t1 ORDER BY val LIMIT 100")?;
            let rows = stmt.query_map([], |_| Ok(()))?;
            rows.count();
        }
        Ok("order by queries".to_string())
    });
    
    format_result("ORDER BY (20 queries)", gongdb_time, rusqlite_time);
}

/// Test: Index performance (if indexes are supported)
#[test]
fn test_index() {
    println!("\n=== Index Performance ===");
    
    let size = 10000;
    
    // Setup with index
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    gongdb.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    // Index might not be fully implemented, ignore errors
    let _ = gongdb.run_statement("CREATE INDEX idx_val ON t1(val)");
    
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        gongdb.run_statement(&sql).unwrap();
    }
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    conn.execute("CREATE TABLE t1(id INTEGER, val INTEGER)", []).unwrap();
    conn.execute("CREATE INDEX idx_val ON t1(val)", []).unwrap();
    
    for i in 0..size {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        conn.execute(&sql, []).unwrap();
    }
    
    // Test: Query using index
    let (gongdb_time, _) = time_it(|| {
        for _ in 0..100 {
            gongdb.run_statement("SELECT * FROM t1 WHERE val = 50")?;
        }
        Ok("index queries".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for _ in 0..100 {
            let mut stmt = conn.prepare("SELECT * FROM t1 WHERE val = 50")?;
            let rows = stmt.query_map([], |_| Ok(()))?;
            rows.count();
        }
        Ok("index queries".to_string())
    });
    
    format_result("Indexed WHERE (100 queries)", gongdb_time, rusqlite_time);
}

/// Test: Mixed workload
#[test]
fn test_mixed_workload() {
    println!("\n=== Mixed Workload Performance ===");
    
    let size = 5000;
    
    // Setup
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    gongdb.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER, name TEXT)").unwrap();
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    conn.execute("CREATE TABLE t1(id INTEGER, val INTEGER, name TEXT)", []).unwrap();
    
    // Test: Mixed operations
    let (gongdb_time, _) = time_it(|| {
        // Insert
        for i in 0..size {
            let sql = format!("INSERT INTO t1 VALUES ({}, {}, 'name{}')", i, i, i);
            gongdb.run_statement(&sql)?;
        }
        
        // Select
        for _ in 0..50 {
            gongdb.run_statement("SELECT COUNT(*) FROM t1 WHERE val < 1000")?;
        }
        
        // Update
        gongdb.run_statement("UPDATE t1 SET val = val + 1 WHERE id < 1000")?;
        
        // Delete
        gongdb.run_statement("DELETE FROM t1 WHERE id >= 4000")?;
        
        // Final select
        gongdb.run_statement("SELECT SUM(val) FROM t1")?;
        
        Ok("mixed workload".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        // Insert
        for i in 0..size {
            let sql = format!("INSERT INTO t1 VALUES ({}, {}, 'name{}')", i, i, i);
            conn.execute(&sql, [])?;
        }
        
        // Select
        for _ in 0..50 {
            let mut stmt = conn.prepare("SELECT COUNT(*) FROM t1 WHERE val < 1000")?;
            stmt.query_row([], |_| Ok(()))?;
        }
        
        // Update
        conn.execute("UPDATE t1 SET val = val + 1 WHERE id < 1000", [])?;
        
        // Delete
        conn.execute("DELETE FROM t1 WHERE id >= 4000", [])?;
        
        // Final select
        let mut stmt = conn.prepare("SELECT SUM(val) FROM t1")?;
        stmt.query_row([], |_| Ok(()))?;
        
        Ok("mixed workload".to_string())
    });
    
    format_result("Mixed workload", gongdb_time, rusqlite_time);
}

/// Run all performance tests
#[test]
fn test_all_performance() {
    test_bulk_insert();
    test_select_where();
    test_aggregation();
    test_join();
    test_update();
    test_delete();
    test_order_by();
    test_index();
    test_mixed_workload();
    
    println!("\n=== Performance Test Summary ===");
    println!("All performance tests completed!");
    println!("Note: Ratio > 1.0 means gongdb is slower, < 1.0 means gongdb is faster");
}
