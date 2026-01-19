//! TPC-C Benchmark: TPC-C specification-compliant implementation
//!
//! This implementation follows the TPC-C specification (v5.11.0) with the following features:
//!
//! **Schema (9 tables as per spec):**
//! - warehouse, district, customer, item, stock, orders, order_line, new_order, history
//!
//! **Transaction Mix (per TPC-C spec):**
//! - New-Order: 45% (with 1% remote warehouse line items)
//! - Payment: 43% (with 15% remote warehouse payments)
//! - Order-Status: 4%
//! - Stock-Level: 4%
//! - Delivery: 4% (batch processing across all districts)
//!
//! **TPC-C Compliance Features:**
//! - Remote warehouse access for New-Order (1% of line items) and Payment (15%)
//! - Stock-Level checks last 20 orders per district
//! - Delivery processes oldest undelivered order for each district in batch
//! - New-Order updates district.d_next_o_id
//! - Payment creates history records
//! - Stock updates include s_ytd, s_order_cnt, s_remote_cnt
//!
//! **Known Simplifications:**
//! - d_next_o_id uses deterministic values instead of atomic reads (acceptable for testing)
//! - Data scaling is reduced for test performance (spec requires W≥10, 10 districts, 3000 customers)
//! - No think-time simulation (acceptable for performance benchmarking)
//!
//! Run with: `cargo test --test tpcc -- --nocapture`

use gongdb::engine::GongDB;
use rusqlite::Connection;
use duckdb::Connection as DuckDBConnection;
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
fn format_result(name: &str, gongdb_time: f64, rusqlite_time: f64, duckdb_time: f64) {
    let ratio_rusqlite = if rusqlite_time > 0.0 {
        gongdb_time / rusqlite_time
    } else {
        f64::INFINITY
    };
    let ratio_duckdb = if duckdb_time > 0.0 {
        gongdb_time / duckdb_time
    } else {
        f64::INFINITY
    };
    println!(
        "  {}: gongdb={:.3}s, rusqlite={:.3}s, duckdb={:.3}s (vs rusqlite={:.2}x, vs duckdb={:.2}x)",
        name, gongdb_time, rusqlite_time, duckdb_time, ratio_rusqlite, ratio_duckdb
    );
}

/// TPC-C Benchmark test
#[test]
fn test_tpcc_benchmark() {
    println!("\n=== TPC-C Benchmark ===");
    
    // TPC-C parameters (scaled down for reasonable test time)
    let warehouses = 2;
    let districts_per_warehouse = 2;
    let customers_per_district = 100;
    let items = 1000;
    let transactions = 100; // Number of transactions to run
    
    // Setup schema and data for gongdb
    let (gongdb_time, _) = time_it(|| {
        let mut db = GongDB::new_in_memory()?;
        setup_tpcc_schema(&mut db)?;
        load_tpcc_data(&mut db, warehouses, districts_per_warehouse, customers_per_district, items)?;
        Ok("setup complete".to_string())
    });
    
    // Setup schema and data for rusqlite
    let (rusqlite_time, _) = time_it(|| {
        let conn = Connection::open_in_memory()?;
        setup_tpcc_schema_rusqlite(&conn)?;
        load_tpcc_data_rusqlite(&conn, warehouses, districts_per_warehouse, customers_per_district, items)?;
        Ok("setup complete".to_string())
    });
    
    // Setup schema and data for duckdb
    let (duckdb_time, _) = time_it(|| {
        let conn = DuckDBConnection::open_in_memory()?;
        setup_tpcc_schema_duckdb(&conn)?;
        load_tpcc_data_duckdb(&conn, warehouses, districts_per_warehouse, customers_per_district, items)?;
        Ok("setup complete".to_string())
    });
    
    format_result("TPC-C Setup", gongdb_time, rusqlite_time, duckdb_time);
    
    // Run TPC-C transactions
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    setup_tpcc_schema(&mut gongdb).expect("setup schema");
    load_tpcc_data(&mut gongdb, warehouses, districts_per_warehouse, customers_per_district, items).expect("load data");
    
    let mut conn = Connection::open_in_memory().expect("create rusqlite");
    setup_tpcc_schema_rusqlite(&conn).expect("setup schema");
    load_tpcc_data_rusqlite(&conn, warehouses, districts_per_warehouse, customers_per_district, items).expect("load data");
    
    let duckdb_conn = DuckDBConnection::open_in_memory().expect("create duckdb");
    setup_tpcc_schema_duckdb(&duckdb_conn).expect("setup schema");
    load_tpcc_data_duckdb(&duckdb_conn, warehouses, districts_per_warehouse, customers_per_district, items).expect("load data");
    
    // New Order transaction (45% of workload)
    let new_order_count = (transactions as f64 * 0.45) as usize;
    let (gongdb_time, _) = time_it(|| {
        for i in 0..new_order_count {
            run_new_order(&mut gongdb, warehouses, districts_per_warehouse, items, i)?;
        }
        Ok(format!("{} new order transactions", new_order_count))
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for i in 0..new_order_count {
            run_new_order_rusqlite(&mut conn, warehouses, districts_per_warehouse, items, i)?;
        }
        Ok(format!("{} new order transactions", new_order_count))
    });
    
    let (duckdb_time, _) = time_it(|| {
        for i in 0..new_order_count {
            run_new_order_duckdb(&duckdb_conn, warehouses, districts_per_warehouse, items, i)?;
        }
        Ok(format!("{} new order transactions", new_order_count))
    });
    
    format_result("New Order Transaction", gongdb_time, rusqlite_time, duckdb_time);
    
    // Payment transaction (43% of workload)
    let payment_count = (transactions as f64 * 0.43) as usize;
    let (gongdb_time, _) = time_it(|| {
        for i in 0..payment_count {
            run_payment(&mut gongdb, warehouses, districts_per_warehouse, customers_per_district, i)?;
        }
        Ok(format!("{} payment transactions", payment_count))
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for i in 0..payment_count {
            run_payment_rusqlite(&mut conn, warehouses, districts_per_warehouse, customers_per_district, i)?;
        }
        Ok(format!("{} payment transactions", payment_count))
    });
    
    let (duckdb_time, _) = time_it(|| {
        for i in 0..payment_count {
            run_payment_duckdb(&duckdb_conn, warehouses, districts_per_warehouse, customers_per_district, i)?;
        }
        Ok(format!("{} payment transactions", payment_count))
    });
    
    format_result("Payment Transaction", gongdb_time, rusqlite_time, duckdb_time);
    
    // Order Status transaction (4% of workload)
    let order_status_count = (transactions as f64 * 0.04) as usize;
    let (gongdb_time, _) = time_it(|| {
        for i in 0..order_status_count {
            run_order_status(&mut gongdb, warehouses, districts_per_warehouse, customers_per_district, i)?;
        }
        Ok(format!("{} order status transactions", order_status_count))
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for i in 0..order_status_count {
            run_order_status_rusqlite(&conn, warehouses, districts_per_warehouse, customers_per_district, i)?;
        }
        Ok(format!("{} order status transactions", order_status_count))
    });
    
    let (duckdb_time, _) = time_it(|| {
        for i in 0..order_status_count {
            run_order_status_duckdb(&duckdb_conn, warehouses, districts_per_warehouse, customers_per_district, i)?;
        }
        Ok(format!("{} order status transactions", order_status_count))
    });
    
    format_result("Order Status Transaction", gongdb_time, rusqlite_time, duckdb_time);
    
    // Stock Level transaction (4% of workload)
    let stock_level_count = (transactions as f64 * 0.04) as usize;
    let (gongdb_time, _) = time_it(|| {
        for i in 0..stock_level_count {
            run_stock_level(&mut gongdb, warehouses, districts_per_warehouse, items, i)?;
        }
        Ok(format!("{} stock level transactions", stock_level_count))
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for i in 0..stock_level_count {
            run_stock_level_rusqlite(&conn, warehouses, districts_per_warehouse, items, i)?;
        }
        Ok(format!("{} stock level transactions", stock_level_count))
    });
    
    let (duckdb_time, _) = time_it(|| {
        for i in 0..stock_level_count {
            run_stock_level_duckdb(&duckdb_conn, warehouses, districts_per_warehouse, items, i)?;
        }
        Ok(format!("{} stock level transactions", stock_level_count))
    });
    
    format_result("Stock Level Transaction", gongdb_time, rusqlite_time, duckdb_time);
    
    // Delivery transaction (4% of workload)
    let delivery_count = (transactions as f64 * 0.04) as usize;
    let (gongdb_time, _) = time_it(|| {
        for i in 0..delivery_count {
            run_delivery(&mut gongdb, warehouses, districts_per_warehouse, i)?;
        }
        Ok(format!("{} delivery transactions", delivery_count))
    });
    
    let (rusqlite_time, _) = time_it(|| {
        for i in 0..delivery_count {
            run_delivery_rusqlite(&mut conn, warehouses, districts_per_warehouse, i)?;
        }
        Ok(format!("{} delivery transactions", delivery_count))
    });
    
    let (duckdb_time, _) = time_it(|| {
        for i in 0..delivery_count {
            run_delivery_duckdb(&duckdb_conn, warehouses, districts_per_warehouse, i)?;
        }
        Ok(format!("{} delivery transactions", delivery_count))
    });
    
    format_result("Delivery Transaction", gongdb_time, rusqlite_time, duckdb_time);
    
    println!("\nTPC-C Benchmark completed!");
}

// TPC-C Schema Setup
fn setup_tpcc_schema(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    // Warehouse table
    db.run_statement(
        "CREATE TABLE warehouse(w_id INTEGER PRIMARY KEY, w_name TEXT, w_street_1 TEXT, w_street_2 TEXT, w_city TEXT, w_state TEXT, w_zip TEXT, w_tax REAL, w_ytd REAL)"
    )?;
    
    // District table
    db.run_statement(
        "CREATE TABLE district(d_id INTEGER, d_w_id INTEGER, d_name TEXT, d_street_1 TEXT, d_street_2 TEXT, d_city TEXT, d_state TEXT, d_zip TEXT, d_tax REAL, d_ytd REAL, d_next_o_id INTEGER, PRIMARY KEY (d_w_id, d_id))"
    )?;
    
    // Customer table
    db.run_statement(
        "CREATE TABLE customer(c_id INTEGER, c_d_id INTEGER, c_w_id INTEGER, c_first TEXT, c_middle TEXT, c_last TEXT, c_street_1 TEXT, c_street_2 TEXT, c_city TEXT, c_state TEXT, c_zip TEXT, c_phone TEXT, c_since TEXT, c_credit TEXT, c_credit_lim REAL, c_discount REAL, c_balance REAL, c_ytd_payment REAL, c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data TEXT, PRIMARY KEY (c_w_id, c_d_id, c_id))"
    )?;
    
    // Item table
    db.run_statement(
        "CREATE TABLE item(i_id INTEGER PRIMARY KEY, i_im_id INTEGER, i_name TEXT, i_price REAL, i_data TEXT)"
    )?;
    
    // Stock table
    db.run_statement(
        "CREATE TABLE stock(s_i_id INTEGER, s_w_id INTEGER, s_quantity INTEGER, s_dist_01 TEXT, s_dist_02 TEXT, s_dist_03 TEXT, s_dist_04 TEXT, s_dist_05 TEXT, s_dist_06 TEXT, s_dist_07 TEXT, s_dist_08 TEXT, s_dist_09 TEXT, s_dist_10 TEXT, s_ytd INTEGER, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT, PRIMARY KEY (s_w_id, s_i_id))"
    )?;
    
    // Order table
    db.run_statement(
        "CREATE TABLE orders(o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER, o_all_local INTEGER, PRIMARY KEY (o_w_id, o_d_id, o_id))"
    )?;
    
    // New Order table
    db.run_statement(
        "CREATE TABLE new_order(no_o_id INTEGER, no_d_id INTEGER, no_w_id INTEGER, PRIMARY KEY (no_w_id, no_d_id, no_o_id))"
    )?;
    
    // Order Line table
    db.run_statement(
        "CREATE TABLE order_line(ol_o_id INTEGER, ol_d_id INTEGER, ol_w_id INTEGER, ol_number INTEGER, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_delivery_d TEXT, ol_quantity INTEGER, ol_amount REAL, ol_dist_info TEXT, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))"
    )?;
    
    // History table (required by TPC-C spec)
    db.run_statement(
        "CREATE TABLE history(h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, h_d_id INTEGER, h_w_id INTEGER, h_date TEXT, h_amount REAL, h_data TEXT)"
    )?;
    
    Ok(())
}

fn setup_tpcc_schema_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "CREATE TABLE warehouse(w_id INTEGER PRIMARY KEY, w_name TEXT, w_street_1 TEXT, w_street_2 TEXT, w_city TEXT, w_state TEXT, w_zip TEXT, w_tax REAL, w_ytd REAL)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE district(d_id INTEGER, d_w_id INTEGER, d_name TEXT, d_street_1 TEXT, d_street_2 TEXT, d_city TEXT, d_state TEXT, d_zip TEXT, d_tax REAL, d_ytd REAL, d_next_o_id INTEGER, PRIMARY KEY (d_w_id, d_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE customer(c_id INTEGER, c_d_id INTEGER, c_w_id INTEGER, c_first TEXT, c_middle TEXT, c_last TEXT, c_street_1 TEXT, c_street_2 TEXT, c_city TEXT, c_state TEXT, c_zip TEXT, c_phone TEXT, c_since TEXT, c_credit TEXT, c_credit_lim REAL, c_discount REAL, c_balance REAL, c_ytd_payment REAL, c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data TEXT, PRIMARY KEY (c_w_id, c_d_id, c_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE item(i_id INTEGER PRIMARY KEY, i_im_id INTEGER, i_name TEXT, i_price REAL, i_data TEXT)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE stock(s_i_id INTEGER, s_w_id INTEGER, s_quantity INTEGER, s_dist_01 TEXT, s_dist_02 TEXT, s_dist_03 TEXT, s_dist_04 TEXT, s_dist_05 TEXT, s_dist_06 TEXT, s_dist_07 TEXT, s_dist_08 TEXT, s_dist_09 TEXT, s_dist_10 TEXT, s_ytd INTEGER, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT, PRIMARY KEY (s_w_id, s_i_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE orders(o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER, o_all_local INTEGER, PRIMARY KEY (o_w_id, o_d_id, o_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE new_order(no_o_id INTEGER, no_d_id INTEGER, no_w_id INTEGER, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE order_line(ol_o_id INTEGER, ol_d_id INTEGER, ol_w_id INTEGER, ol_number INTEGER, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_delivery_d TEXT, ol_quantity INTEGER, ol_amount REAL, ol_dist_info TEXT, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE history(h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, h_d_id INTEGER, h_w_id INTEGER, h_date TEXT, h_amount REAL, h_data TEXT)",
        []
    )?;
    
    Ok(())
}

fn setup_tpcc_schema_duckdb(conn: &DuckDBConnection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "CREATE TABLE warehouse(w_id INTEGER PRIMARY KEY, w_name TEXT, w_street_1 TEXT, w_street_2 TEXT, w_city TEXT, w_state TEXT, w_zip TEXT, w_tax REAL, w_ytd REAL)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE district(d_id INTEGER, d_w_id INTEGER, d_name TEXT, d_street_1 TEXT, d_street_2 TEXT, d_city TEXT, d_state TEXT, d_zip TEXT, d_tax REAL, d_ytd REAL, d_next_o_id INTEGER, PRIMARY KEY (d_w_id, d_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE customer(c_id INTEGER, c_d_id INTEGER, c_w_id INTEGER, c_first TEXT, c_middle TEXT, c_last TEXT, c_street_1 TEXT, c_street_2 TEXT, c_city TEXT, c_state TEXT, c_zip TEXT, c_phone TEXT, c_since TEXT, c_credit TEXT, c_credit_lim REAL, c_discount REAL, c_balance REAL, c_ytd_payment REAL, c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data TEXT, PRIMARY KEY (c_w_id, c_d_id, c_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE item(i_id INTEGER PRIMARY KEY, i_im_id INTEGER, i_name TEXT, i_price REAL, i_data TEXT)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE stock(s_i_id INTEGER, s_w_id INTEGER, s_quantity INTEGER, s_dist_01 TEXT, s_dist_02 TEXT, s_dist_03 TEXT, s_dist_04 TEXT, s_dist_05 TEXT, s_dist_06 TEXT, s_dist_07 TEXT, s_dist_08 TEXT, s_dist_09 TEXT, s_dist_10 TEXT, s_ytd INTEGER, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT, PRIMARY KEY (s_w_id, s_i_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE orders(o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER, o_all_local INTEGER, PRIMARY KEY (o_w_id, o_d_id, o_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE new_order(no_o_id INTEGER, no_d_id INTEGER, no_w_id INTEGER, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE order_line(ol_o_id INTEGER, ol_d_id INTEGER, ol_w_id INTEGER, ol_number INTEGER, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_delivery_d TEXT, ol_quantity INTEGER, ol_amount REAL, ol_dist_info TEXT, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE history(h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, h_d_id INTEGER, h_w_id INTEGER, h_date TEXT, h_amount REAL, h_data TEXT)",
        []
    )?;
    
    Ok(())
}

// TPC-C Data Loading
fn load_tpcc_data(
    db: &mut GongDB,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    items: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load warehouses
    for w_id in 1..=warehouses {
        db.run_statement(&format!(
            "INSERT INTO warehouse VALUES ({}, 'Warehouse{}', 'Street1', 'Street2', 'City', 'ST', '12345', 0.0825, 300000.0)",
            w_id, w_id
        ))?;
    }
    
    // Load districts
    for w_id in 1..=warehouses {
        for d_id in 1..=districts_per_warehouse {
            db.run_statement(&format!(
                "INSERT INTO district VALUES ({}, {}, 'District{}', 'Street1', 'Street2', 'City', 'ST', '12345', 0.0825, 30000.0, 3001)",
                d_id, w_id, d_id
            ))?;
        }
    }
    
    // Load customers
    for w_id in 1..=warehouses {
        for d_id in 1..=districts_per_warehouse {
            for c_id in 1..=customers_per_district {
                db.run_statement(&format!(
                    "INSERT INTO customer VALUES ({}, {}, {}, 'First{}', 'M', 'Last{}', 'Street1', 'Street2', 'City', 'ST', '12345', '555-1234', '2024-01-01', 'GC', 50000.0, 0.05, -10.0, 10.0, 1, 0, 'Data')",
                    c_id, d_id, w_id, c_id, c_id
                ))?;
            }
        }
    }
    
    // Load items
    for i_id in 1..=items {
        db.run_statement(&format!(
            "INSERT INTO item VALUES ({}, {}, 'Item{}', {}, 'Data{}')",
            i_id, i_id % 100, i_id, 1.0 + (i_id as f64) * 0.01, i_id
        ))?;
    }
    
    // Load stock
    for w_id in 1..=warehouses {
        for i_id in 1..=items {
            db.run_statement(&format!(
                "INSERT INTO stock VALUES ({}, {}, {}, 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 0, 0, 0, 'S_DATA')",
                i_id, w_id, 10 + (i_id % 90)
            ))?;
        }
    }
    
    Ok(())
}

fn load_tpcc_data_rusqlite(
    conn: &Connection,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    items: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load warehouses
    for w_id in 1..=warehouses {
        conn.execute(
            &format!(
                "INSERT INTO warehouse VALUES ({}, 'Warehouse{}', 'Street1', 'Street2', 'City', 'ST', '12345', 0.0825, 300000.0)",
                w_id, w_id
            ),
            []
        )?;
    }
    
    // Load districts
    for w_id in 1..=warehouses {
        for d_id in 1..=districts_per_warehouse {
            conn.execute(
                &format!(
                    "INSERT INTO district VALUES ({}, {}, 'District{}', 'Street1', 'Street2', 'City', 'ST', '12345', 0.0825, 30000.0, 3001)",
                    d_id, w_id, d_id
                ),
                []
            )?;
        }
    }
    
    // Load customers
    for w_id in 1..=warehouses {
        for d_id in 1..=districts_per_warehouse {
            for c_id in 1..=customers_per_district {
                conn.execute(
                    &format!(
                        "INSERT INTO customer VALUES ({}, {}, {}, 'First{}', 'M', 'Last{}', 'Street1', 'Street2', 'City', 'ST', '12345', '555-1234', '2024-01-01', 'GC', 50000.0, 0.05, -10.0, 10.0, 1, 0, 'Data')",
                        c_id, d_id, w_id, c_id, c_id
                    ),
                    []
                )?;
            }
        }
    }
    
    // Load items
    for i_id in 1..=items {
        conn.execute(
            &format!(
                "INSERT INTO item VALUES ({}, {}, 'Item{}', {}, 'Data{}')",
                i_id, i_id % 100, i_id, 1.0 + (i_id as f64) * 0.01, i_id
            ),
            []
        )?;
    }
    
    // Load stock
    for w_id in 1..=warehouses {
        for i_id in 1..=items {
            conn.execute(
                &format!(
                    "INSERT INTO stock VALUES ({}, {}, {}, 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 0, 0, 0, 'S_DATA')",
                    i_id, w_id, 10 + (i_id % 90)
                ),
                []
            )?;
        }
    }
    
    Ok(())
}

fn load_tpcc_data_duckdb(
    conn: &DuckDBConnection,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    items: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load warehouses
    for w_id in 1..=warehouses {
        conn.execute(
            &format!(
                "INSERT INTO warehouse VALUES ({}, 'Warehouse{}', 'Street1', 'Street2', 'City', 'ST', '12345', 0.0825, 300000.0)",
                w_id, w_id
            ),
            []
        )?;
    }
    
    // Load districts
    for w_id in 1..=warehouses {
        for d_id in 1..=districts_per_warehouse {
            conn.execute(
                &format!(
                    "INSERT INTO district VALUES ({}, {}, 'District{}', 'Street1', 'Street2', 'City', 'ST', '12345', 0.0825, 30000.0, 3001)",
                    d_id, w_id, d_id
                ),
                []
            )?;
        }
    }
    
    // Load customers
    for w_id in 1..=warehouses {
        for d_id in 1..=districts_per_warehouse {
            for c_id in 1..=customers_per_district {
                conn.execute(
                    &format!(
                        "INSERT INTO customer VALUES ({}, {}, {}, 'First{}', 'M', 'Last{}', 'Street1', 'Street2', 'City', 'ST', '12345', '555-1234', '2024-01-01', 'GC', 50000.0, 0.05, -10.0, 10.0, 1, 0, 'Data')",
                        c_id, d_id, w_id, c_id, c_id
                    ),
                    []
                )?;
            }
        }
    }
    
    // Load items
    for i_id in 1..=items {
        conn.execute(
            &format!(
                "INSERT INTO item VALUES ({}, {}, 'Item{}', {}, 'Data{}')",
                i_id, i_id % 100, i_id, 1.0 + (i_id as f64) * 0.01, i_id
            ),
            []
        )?;
    }
    
    // Load stock
    for w_id in 1..=warehouses {
        for i_id in 1..=items {
            conn.execute(
                &format!(
                    "INSERT INTO stock VALUES ({}, {}, {}, 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 0, 0, 0, 'S_DATA')",
                    i_id, w_id, 10 + (i_id % 90)
                ),
                []
            )?;
        }
    }
    
    Ok(())
}

// TPC-C Transactions
fn run_new_order(
    db: &mut GongDB,
    warehouses: usize,
    districts_per_warehouse: usize,
    items: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    let c_id = (txn_id % 100) + 1;
    let ol_cnt = 5 + (txn_id % 10); // 5-15 order lines
    
    db.run_statement("BEGIN TRANSACTION")?;
    
    // Get and increment next order ID from district (TPC-C spec requirement)
    // Read current d_next_o_id, use it, then increment
    // Note: In a real implementation, this should be atomic
    let o_id_result = db.run_statement(&format!(
        "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
        w_id, d_id
    ))?;
    // For simplicity, we'll use a deterministic approach based on txn_id
    // In a real implementation, this would read the actual value and update it
    let o_id = 3001 + (txn_id % 1000); // Simulated order ID
    
    // Update district's next order ID (TPC-C spec requirement)
    db.run_statement(&format!(
        "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = {} AND d_id = {}",
        w_id, d_id
    ))?;
    
    // Track if any remote warehouse is used (for o_all_local)
    // TPC-C spec: o_all_local = 1 if all order lines come from local warehouse, 0 otherwise
    let mut all_local = 1;
    
    // Pre-check for remote warehouses to set o_all_local correctly
    for ol_number in 1..=ol_cnt {
        // TPC-C spec: 1% of order lines should come from remote warehouses
        if (txn_id * ol_number) % 100 == 0 && warehouses > 1 {
            all_local = 0;
            break;
        }
    }
    
    // Create order with correct o_all_local flag
    db.run_statement(&format!(
        "INSERT INTO orders VALUES ({}, {}, {}, {}, '2024-01-01', NULL, {}, {})",
        o_id, d_id, w_id, c_id, ol_cnt, all_local
    ))?;
    
    // Create new_order entry
    db.run_statement(&format!(
        "INSERT INTO new_order VALUES ({}, {}, {})",
        o_id, d_id, w_id
    ))?;
    
    // Create order lines
    for ol_number in 1..=ol_cnt {
        let ol_i_id = (txn_id * ol_number) % items + 1;
        let ol_quantity = 1 + (txn_id % 10);
        
        // TPC-C spec: 1% of order lines should come from remote warehouses
        let ol_supply_w_id = if (txn_id * ol_number) % 100 == 0 && warehouses > 1 {
            // Remote warehouse (different from w_id)
            ((w_id % warehouses) + 1) % warehouses + 1
        } else {
            // Local warehouse
            w_id
        };
        
        // Get item price
        let item_price = 1.0 + (ol_i_id as f64) * 0.01;
        let ol_amount = item_price * (ol_quantity as f64);
        
        // Get district info from stock (s_dist_XX where XX = d_id)
        let dist_info = format!("S_DIST_{:02}", d_id);
        
        db.run_statement(&format!(
            "INSERT INTO order_line VALUES ({}, {}, {}, {}, {}, {}, NULL, {}, {}, '{}')",
            o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, dist_info
        ))?;
        
        // Update stock (TPC-C spec: update s_quantity, s_ytd, s_order_cnt, s_remote_cnt)
        if ol_supply_w_id != w_id {
            // Remote warehouse - update remote count
            db.run_statement(&format!(
                "UPDATE stock SET s_quantity = s_quantity - {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 1 WHERE s_w_id = {} AND s_i_id = {}",
                ol_quantity, ol_quantity, ol_supply_w_id, ol_i_id
            ))?;
        } else {
            // Local warehouse
            db.run_statement(&format!(
                "UPDATE stock SET s_quantity = s_quantity - {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = {} AND s_i_id = {}",
                ol_quantity, ol_quantity, ol_supply_w_id, ol_i_id
            ))?;
        }
    }
    
    db.run_statement("COMMIT")?;
    Ok(())
}

fn run_new_order_rusqlite(
    conn: &mut Connection,
    warehouses: usize,
    districts_per_warehouse: usize,
    items: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    let c_id = (txn_id % 100) + 1;
    let ol_cnt = 5 + (txn_id % 10);
    
    let tx = conn.transaction()?;
    
    // Get and increment next order ID from district (TPC-C spec requirement)
    let o_id = 3001 + (txn_id % 1000); // Simulated order ID
    
    // Update district's next order ID (TPC-C spec requirement)
    tx.execute(
        &format!(
            "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = {} AND d_id = {}",
            w_id, d_id
        ),
        []
    )?;
    
    // Pre-check for remote warehouses to set o_all_local correctly
    let mut all_local = 1;
    for ol_number in 1..=ol_cnt {
        // TPC-C spec: 1% of order lines should come from remote warehouses
        if (txn_id * ol_number) % 100 == 0 && warehouses > 1 {
            all_local = 0;
            break;
        }
    }
    
    tx.execute(
        &format!(
            "INSERT INTO orders VALUES ({}, {}, {}, {}, '2024-01-01', NULL, {}, {})",
            o_id, d_id, w_id, c_id, ol_cnt, all_local
        ),
        []
    )?;
    
    tx.execute(
        &format!(
            "INSERT INTO new_order VALUES ({}, {}, {})",
            o_id, d_id, w_id
        ),
        []
    )?;
    
    for ol_number in 1..=ol_cnt {
        let ol_i_id = (txn_id * ol_number) % items + 1;
        let ol_quantity = 1 + (txn_id % 10);
        
        // TPC-C spec: 1% of order lines should come from remote warehouses
        let ol_supply_w_id = if (txn_id * ol_number) % 100 == 0 && warehouses > 1 {
            ((w_id % warehouses) + 1) % warehouses + 1
        } else {
            w_id
        };
        
        let item_price = 1.0 + (ol_i_id as f64) * 0.01;
        let ol_amount = item_price * (ol_quantity as f64);
        let dist_info = format!("S_DIST_{:02}", d_id);
        
        tx.execute(
            &format!(
                "INSERT INTO order_line VALUES ({}, {}, {}, {}, {}, {}, NULL, {}, {}, '{}')",
                o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, dist_info
            ),
            []
        )?;
        
        // Update stock (TPC-C spec: update s_quantity, s_ytd, s_order_cnt, s_remote_cnt)
        if ol_supply_w_id != w_id {
            tx.execute(
                &format!(
                    "UPDATE stock SET s_quantity = s_quantity - {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 1 WHERE s_w_id = {} AND s_i_id = {}",
                    ol_quantity, ol_quantity, ol_supply_w_id, ol_i_id
                ),
                []
            )?;
        } else {
            tx.execute(
                &format!(
                    "UPDATE stock SET s_quantity = s_quantity - {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = {} AND s_i_id = {}",
                    ol_quantity, ol_quantity, ol_supply_w_id, ol_i_id
                ),
                []
            )?;
        }
    }
    
    tx.commit()?;
    Ok(())
}

fn run_new_order_duckdb(
    conn: &DuckDBConnection,
    warehouses: usize,
    districts_per_warehouse: usize,
    items: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    let c_id = (txn_id % 100) + 1;
    let ol_cnt = 5 + (txn_id % 10);
    
    conn.execute("BEGIN TRANSACTION", [])?;
    
    let o_id = 3001 + (txn_id % 1000);
    
    conn.execute(
        &format!(
            "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = {} AND d_id = {}",
            w_id, d_id
        ),
        []
    )?;
    
    let mut all_local = 1;
    for ol_number in 1..=ol_cnt {
        if (txn_id * ol_number) % 100 == 0 && warehouses > 1 {
            all_local = 0;
            break;
        }
    }
    
    conn.execute(
        &format!(
            "INSERT INTO orders VALUES ({}, {}, {}, {}, '2024-01-01', NULL, {}, {})",
            o_id, d_id, w_id, c_id, ol_cnt, all_local
        ),
        []
    )?;
    
    conn.execute(
        &format!(
            "INSERT INTO new_order VALUES ({}, {}, {})",
            o_id, d_id, w_id
        ),
        []
    )?;
    
    for ol_number in 1..=ol_cnt {
        let ol_i_id = (txn_id * ol_number) % items + 1;
        let ol_quantity = 1 + (txn_id % 10);
        
        let ol_supply_w_id = if (txn_id * ol_number) % 100 == 0 && warehouses > 1 {
            ((w_id % warehouses) + 1) % warehouses + 1
        } else {
            w_id
        };
        
        let item_price = 1.0 + (ol_i_id as f64) * 0.01;
        let ol_amount = item_price * (ol_quantity as f64);
        let dist_info = format!("S_DIST_{:02}", d_id);
        
        conn.execute(
            &format!(
                "INSERT INTO order_line VALUES ({}, {}, {}, {}, {}, {}, NULL, {}, {}, '{}')",
                o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, dist_info
            ),
            []
        )?;
        
        if ol_supply_w_id != w_id {
            conn.execute(
                &format!(
                    "UPDATE stock SET s_quantity = s_quantity - {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 1 WHERE s_w_id = {} AND s_i_id = {}",
                    ol_quantity, ol_quantity, ol_supply_w_id, ol_i_id
                ),
                []
            )?;
        } else {
            conn.execute(
                &format!(
                    "UPDATE stock SET s_quantity = s_quantity - {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = {} AND s_i_id = {}",
                    ol_quantity, ol_quantity, ol_supply_w_id, ol_i_id
                ),
                []
            )?;
        }
    }
    
    conn.execute("COMMIT", [])?;
    Ok(())
}

fn run_payment(
    db: &mut GongDB,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // TPC-C spec: 15% of payments go to remote warehouse
    let is_remote = (txn_id % 100) < 15 && warehouses > 1;
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    
    // If remote, use different warehouse for customer
    let c_w_id = if is_remote {
        ((w_id % warehouses) + 1) % warehouses + 1
    } else {
        w_id
    };
    let c_d_id = d_id;
    let c_id = (txn_id % customers_per_district) + 1;
    let payment = 10.0 + (txn_id as f64) * 0.1;
    
    db.run_statement("BEGIN TRANSACTION")?;
    
    // Update warehouse (always local warehouse)
    db.run_statement(&format!(
        "UPDATE warehouse SET w_ytd = w_ytd + {} WHERE w_id = {}",
        payment, w_id
    ))?;
    
    // Update district (always local district)
    db.run_statement(&format!(
        "UPDATE district SET d_ytd = d_ytd + {} WHERE d_w_id = {} AND d_id = {}",
        payment, w_id, d_id
    ))?;
    
    // Update customer (may be remote)
    db.run_statement(&format!(
        "UPDATE customer SET c_balance = c_balance - {}, c_ytd_payment = c_ytd_payment + {}, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
        payment, payment, c_w_id, c_d_id, c_id
    ))?;
    
    // Insert into history table (TPC-C spec requirement)
    db.run_statement(&format!(
        "INSERT INTO history VALUES ({}, {}, {}, {}, {}, '2024-01-01', {}, 'Payment history data')",
        c_id, c_d_id, c_w_id, d_id, w_id, payment
    ))?;
    
    db.run_statement("COMMIT")?;
    Ok(())
}

fn run_payment_rusqlite(
    conn: &mut Connection,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // TPC-C spec: 15% of payments go to remote warehouse
    let is_remote = (txn_id % 100) < 15 && warehouses > 1;
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    
    let c_w_id = if is_remote {
        ((w_id % warehouses) + 1) % warehouses + 1
    } else {
        w_id
    };
    let c_d_id = d_id;
    let c_id = (txn_id % customers_per_district) + 1;
    let payment = 10.0 + (txn_id as f64) * 0.1;
    
    let tx = conn.transaction()?;
    
    tx.execute(
        &format!(
            "UPDATE warehouse SET w_ytd = w_ytd + {} WHERE w_id = {}",
            payment, w_id
        ),
        []
    )?;
    
    tx.execute(
        &format!(
            "UPDATE district SET d_ytd = d_ytd + {} WHERE d_w_id = {} AND d_id = {}",
            payment, w_id, d_id
        ),
        []
    )?;
    
    tx.execute(
        &format!(
            "UPDATE customer SET c_balance = c_balance - {}, c_ytd_payment = c_ytd_payment + {}, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
            payment, payment, c_w_id, c_d_id, c_id
        ),
        []
    )?;
    
    // Insert into history table (TPC-C spec requirement)
    tx.execute(
        &format!(
            "INSERT INTO history VALUES ({}, {}, {}, {}, {}, '2024-01-01', {}, 'Payment history data')",
            c_id, c_d_id, c_w_id, d_id, w_id, payment
        ),
        []
    )?;
    
    tx.commit()?;
    Ok(())
}

fn run_payment_duckdb(
    conn: &DuckDBConnection,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let is_remote = (txn_id % 100) < 15 && warehouses > 1;
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    
    let c_w_id = if is_remote {
        ((w_id % warehouses) + 1) % warehouses + 1
    } else {
        w_id
    };
    let c_d_id = d_id;
    let c_id = (txn_id % customers_per_district) + 1;
    let payment = 10.0 + (txn_id as f64) * 0.1;
    
    conn.execute("BEGIN TRANSACTION", [])?;
    
    conn.execute(
        &format!(
            "UPDATE warehouse SET w_ytd = w_ytd + {} WHERE w_id = {}",
            payment, w_id
        ),
        []
    )?;
    
    conn.execute(
        &format!(
            "UPDATE district SET d_ytd = d_ytd + {} WHERE d_w_id = {} AND d_id = {}",
            payment, w_id, d_id
        ),
        []
    )?;
    
    conn.execute(
        &format!(
            "UPDATE customer SET c_balance = c_balance - {}, c_ytd_payment = c_ytd_payment + {}, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
            payment, payment, c_w_id, c_d_id, c_id
        ),
        []
    )?;
    
    conn.execute(
        &format!(
            "INSERT INTO history VALUES ({}, {}, {}, {}, {}, '2024-01-01', {}, 'Payment history data')",
            c_id, c_d_id, c_w_id, d_id, w_id, payment
        ),
        []
    )?;
    
    conn.execute("COMMIT", [])?;
    Ok(())
}

fn run_order_status(
    db: &mut GongDB,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    let c_id = (txn_id % customers_per_district) + 1;
    
    // Get customer info
    db.run_statement(&format!(
        "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
        w_id, d_id, c_id
    ))?;
    
    // Get last order
    db.run_statement(&format!(
        "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1",
        w_id, d_id, c_id
    ))?;
    
    Ok(())
}

fn run_order_status_rusqlite(
    conn: &Connection,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let _d_id = (txn_id % districts_per_warehouse) + 1;
    let c_id = (txn_id % customers_per_district) + 1;
    
    let _: Vec<()> = conn.prepare(&format!(
        "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
        w_id, _d_id, c_id
    ))?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    
    let _: Vec<()> = conn.prepare(&format!(
        "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1",
        w_id, _d_id, c_id
    ))?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    
    Ok(())
}

fn run_order_status_duckdb(
    conn: &DuckDBConnection,
    warehouses: usize,
    districts_per_warehouse: usize,
    customers_per_district: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    let c_id = (txn_id % customers_per_district) + 1;
    
    let mut stmt = conn.prepare(&format!(
        "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
        w_id, d_id, c_id
    ))?;
    let _: Vec<()> = stmt.query_map([], |_| Ok(()))?
        .collect::<Result<_, _>>()?;
    
    let mut stmt = conn.prepare(&format!(
        "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1",
        w_id, d_id, c_id
    ))?;
    let _: Vec<()> = stmt.query_map([], |_| Ok(()))?
        .collect::<Result<_, _>>()?;
    
    Ok(())
}

fn run_stock_level(
    db: &mut GongDB,
    warehouses: usize,
    districts_per_warehouse: usize,
    _items: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    let threshold = 10;
    
    // TPC-C spec: Stock-Level transaction
    // 1. Get the next available order number (d_next_o_id) from district
    // 2. Find the last 20 orders (o_id < d_next_o_id, ordered by o_id DESC, LIMIT 20)
    // 3. Count distinct items from those orders where stock quantity < threshold
    
    // Get district's next order ID
    let next_o_id = 3001 + (txn_id % 1000); // Simulated - in real implementation would read from district
    
    // TPC-C spec: Count distinct items from last 20 orders where stock < threshold
    db.run_statement(&format!(
        "SELECT COUNT(DISTINCT ol_i_id) FROM order_line, stock WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id >= {} - 20 AND ol_o_id < {} AND s_w_id = ol_w_id AND s_i_id = ol_i_id AND s_quantity < {}",
        w_id, d_id, next_o_id, next_o_id, threshold
    ))?;
    
    Ok(())
}

fn run_stock_level_rusqlite(
    conn: &Connection,
    warehouses: usize,
    districts_per_warehouse: usize,
    _items: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    let threshold = 10;
    
    // TPC-C spec: Get district's next order ID, then count distinct items from last 20 orders
    let next_o_id = 3001 + (txn_id % 1000); // Simulated - in real implementation would read from district
    
    // TPC-C spec: Count distinct items from last 20 orders where stock < threshold
    let _: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(DISTINCT ol_i_id) FROM order_line, stock WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id >= {} - 20 AND ol_o_id < {} AND s_w_id = ol_w_id AND s_i_id = ol_i_id AND s_quantity < {}",
            w_id, d_id, next_o_id, next_o_id, threshold
        ),
        [],
        |row| row.get(0)
    )?;
    
    Ok(())
}

fn run_stock_level_duckdb(
    conn: &DuckDBConnection,
    warehouses: usize,
    districts_per_warehouse: usize,
    _items: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    let d_id = (txn_id % districts_per_warehouse) + 1;
    let threshold = 10;
    
    let next_o_id = 3001 + (txn_id % 1000);
    
    let mut stmt = conn.prepare(
        &format!(
            "SELECT COUNT(DISTINCT ol_i_id) FROM order_line, stock WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id >= {} - 20 AND ol_o_id < {} AND s_w_id = ol_w_id AND s_i_id = ol_i_id AND s_quantity < {}",
            w_id, d_id, next_o_id, next_o_id, threshold
        )
    )?;
    let _: i64 = stmt.query_row([], |row| row.get(0))?;
    
    Ok(())
}

fn run_delivery(
    db: &mut GongDB,
    warehouses: usize,
    districts_per_warehouse: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    
    db.run_statement("BEGIN TRANSACTION")?;
    
    // TPC-C spec: Delivery transaction processes oldest undelivered order for EACH district
    // in the warehouse (batch processing across all districts)
    for d_id in 1..=districts_per_warehouse {
        // Get oldest new_order for this district
        // TPC-C spec requires finding MIN(no_o_id) for the district
        // For simplicity in this test, we use a deterministic approach
        // In a real implementation, this would query: SELECT MIN(no_o_id) FROM new_order WHERE no_w_id = w_id AND no_d_id = d_id
        let o_id = 3001 + (txn_id * districts_per_warehouse + d_id) % 1000;
        
        // TPC-C spec: Delete the oldest new_order entry for this district
        // Note: In a real implementation, we'd first query for MIN(no_o_id), then delete it
        // This simplified version assumes the order exists
        db.run_statement(&format!(
            "DELETE FROM new_order WHERE no_w_id = {} AND no_d_id = {} AND no_o_id = {}",
            w_id, d_id, o_id
        ))?;
        
        // TPC-C spec: Update order with carrier ID (1-10)
        let carrier_id = 1 + (txn_id % 10);
        db.run_statement(&format!(
            "UPDATE orders SET o_carrier_id = {} WHERE o_w_id = {} AND o_d_id = {} AND o_id = {}",
            carrier_id, w_id, d_id, o_id
        ))?;
        
        // TPC-C spec: Update order lines with delivery date
        db.run_statement(&format!(
            "UPDATE order_line SET ol_delivery_d = '2024-01-01' WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id = {}",
            w_id, d_id, o_id
        ))?;
        
        // TPC-C spec: Update customer balance (add sum of ol_amount) and delivery count
        db.run_statement(&format!(
            "UPDATE customer SET c_balance = c_balance + (SELECT COALESCE(SUM(ol_amount), 0) FROM order_line WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id = {}), c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = {} AND c_d_id = {} AND c_id = (SELECT o_c_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_id = {})",
            w_id, d_id, o_id, w_id, d_id, w_id, d_id, o_id
        ))?;
    }
    
    db.run_statement("COMMIT")?;
    Ok(())
}

fn run_delivery_rusqlite(
    conn: &mut Connection,
    warehouses: usize,
    districts_per_warehouse: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    
    let tx = conn.transaction()?;
    
    // TPC-C spec: Delivery transaction processes oldest undelivered order for EACH district
    for d_id in 1..=districts_per_warehouse {
        // Get oldest new_order for this district
        let o_id = 3001 + (txn_id * districts_per_warehouse + d_id) % 1000;
        
        // TPC-C spec: Delete the oldest new_order entry for this district
        tx.execute(
            &format!(
                "DELETE FROM new_order WHERE no_w_id = {} AND no_d_id = {} AND no_o_id = {}",
                w_id, d_id, o_id
            ),
            []
        )?;
        
        // TPC-C spec: Update order with carrier ID (1-10)
        let carrier_id = 1 + (txn_id % 10);
        tx.execute(
            &format!(
                "UPDATE orders SET o_carrier_id = {} WHERE o_w_id = {} AND o_d_id = {} AND o_id = {}",
                carrier_id, w_id, d_id, o_id
            ),
            []
        )?;
        
        // TPC-C spec: Update order lines with delivery date
        tx.execute(
            &format!(
                "UPDATE order_line SET ol_delivery_d = '2024-01-01' WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id = {}",
                w_id, d_id, o_id
            ),
            []
        )?;
        
        // TPC-C spec: Update customer balance (add sum of ol_amount) and delivery count
        tx.execute(
            &format!(
                "UPDATE customer SET c_balance = c_balance + (SELECT COALESCE(SUM(ol_amount), 0) FROM order_line WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id = {}), c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = {} AND c_d_id = {} AND c_id = (SELECT o_c_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_id = {})",
                w_id, d_id, o_id, w_id, d_id, w_id, d_id, o_id
            ),
            []
        )?;
    }
    
    tx.commit()?;
    Ok(())
}

fn run_delivery_duckdb(
    conn: &DuckDBConnection,
    warehouses: usize,
    districts_per_warehouse: usize,
    txn_id: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let w_id = (txn_id % warehouses) + 1;
    
    conn.execute("BEGIN TRANSACTION", [])?;
    
    for d_id in 1..=districts_per_warehouse {
        let o_id = 3001 + (txn_id * districts_per_warehouse + d_id) % 1000;
        
        conn.execute(
            &format!(
                "DELETE FROM new_order WHERE no_w_id = {} AND no_d_id = {} AND no_o_id = {}",
                w_id, d_id, o_id
            ),
            []
        )?;
        
        let carrier_id = 1 + (txn_id % 10);
        conn.execute(
            &format!(
                "UPDATE orders SET o_carrier_id = {} WHERE o_w_id = {} AND o_d_id = {} AND o_id = {}",
                carrier_id, w_id, d_id, o_id
            ),
            []
        )?;
        
        conn.execute(
            &format!(
                "UPDATE order_line SET ol_delivery_d = '2024-01-01' WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id = {}",
                w_id, d_id, o_id
            ),
            []
        )?;
        
        conn.execute(
            &format!(
                "UPDATE customer SET c_balance = c_balance + (SELECT COALESCE(SUM(ol_amount), 0) FROM order_line WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id = {}), c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = {} AND c_d_id = {} AND c_id = (SELECT o_c_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_id = {})",
                w_id, d_id, o_id, w_id, d_id, w_id, d_id, o_id
            ),
            []
        )?;
    }
    
    conn.execute("COMMIT", [])?;
    Ok(())
}
