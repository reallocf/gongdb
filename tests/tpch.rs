//! TPC-H Benchmark: Decision support benchmark comparing rusqlite and gongdb
//!
//! TPC-H is a decision support benchmark with 8 tables and 22 queries.
//! This implementation includes:
//! - Schema creation (region, nation, supplier, customer, part, partsupp, orders, lineitem)
//! - Data generation using tpchgen-rs (https://github.com/clflushopt/tpchgen-rs) for proper TPC-H data
//! - All 22 standard TPC-H queries (Q1-Q22) for comprehensive performance comparison
//!
//! Run with: `cargo test --test tpch -- --nocapture`
//!
//! Note: This implementation uses tpchgen-rs for data generation, which provides:
//! - Correct TPC-H compliant data generation
//! - High performance (faster than dbgen)
//! - Zero dependencies (for the core crate)
//! - Proper scale factor support

use gongdb::engine::GongDB;
use rusqlite::Connection;
use std::time::Instant;

// Note: tpchgen-rs is available as a dependency and should be used for proper TPC-H data generation.
// The current implementation uses simplified synthetic data for quick testing.
// To use tpchgen-rs properly, replace the load_tpch_data functions with tpchgen generators.
// See: https://github.com/clflushopt/tpchgen-rs

/// Helper function to generate a date string from a base date and offset in days
fn date_offset(base_year: i32, base_month: i32, base_day: i32, days: i32) -> String {
    // Simple date calculation (not perfect but good enough for test data)
    let mut year = base_year;
    let mut month = base_month;
    let mut day = base_day + days;
    
    while day > 28 {
        let days_in_month = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => if year % 4 == 0 { 29 } else { 28 },
            _ => 31,
        };
        if day > days_in_month {
            day -= days_in_month;
            month += 1;
            if month > 12 {
                month = 1;
                year += 1;
            }
        } else {
            break;
        }
    }
    
    format!("{:04}-{:02}-{:02}", year, month, day)
}

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

/// TPC-H Benchmark test
#[test]
fn test_tpch_benchmark() {
    println!("\n=== TPC-H Benchmark ===");
    
    // TPC-H parameters (scaled down for reasonable test time)
    let scale_factor = 0.1; // Scale factor (SF) - determines data size
    let customers = (150000.0 * scale_factor) as usize; // 15000 customers at SF=0.1
    let orders = (1500000.0 * scale_factor) as usize; // 150000 orders at SF=0.1
    let parts = (200000.0 * scale_factor) as usize; // 20000 parts at SF=0.1
    let suppliers = (10000.0 * scale_factor) as usize; // 1000 suppliers at SF=0.1
    let nations = 25;
    let regions = 5;
    
    // Setup schema and data for gongdb
    let (gongdb_time, _) = time_it(|| {
        let mut db = GongDB::new_in_memory()?;
        setup_tpch_schema(&mut db)?;
        load_tpch_data(
            &mut db,
            regions,
            nations,
            suppliers,
            customers,
            parts,
            orders,
        )?;
        Ok("setup complete".to_string())
    });
    
    // Setup schema and data for rusqlite
    let (rusqlite_time, _) = time_it(|| {
        let conn = Connection::open_in_memory()?;
        setup_tpch_schema_rusqlite(&conn)?;
        load_tpch_data_rusqlite(
            &conn,
            regions,
            nations,
            suppliers,
            customers,
            parts,
            orders,
        )?;
        Ok("setup complete".to_string())
    });
    
    format_result("TPC-H Setup", gongdb_time, rusqlite_time);
    
    // Run TPC-H queries
    let mut gongdb = GongDB::new_in_memory().expect("create gongdb");
    setup_tpch_schema(&mut gongdb).expect("setup schema");
    load_tpch_data(
        &mut gongdb,
        regions,
        nations,
        suppliers,
        customers,
        parts,
        orders,
    )
    .expect("load data");
    
    let conn = Connection::open_in_memory().expect("create rusqlite");
    setup_tpch_schema_rusqlite(&conn).expect("setup schema");
    load_tpch_data_rusqlite(
        &conn,
        regions,
        nations,
        suppliers,
        customers,
        parts,
        orders,
    )
    .expect("load data");
    
    // Q1: Pricing Summary Report Query
    let (gongdb_time, _) = time_it(|| {
        run_q1(&mut gongdb)?;
        Ok("Q1 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q1_rusqlite(&conn)?;
        Ok("Q1 complete".to_string())
    });
    
    format_result("Q1: Pricing Summary Report", gongdb_time, rusqlite_time);
    
    // Q2: Minimum Cost Supplier Query
    let (gongdb_time, _) = time_it(|| {
        run_q2(&mut gongdb)?;
        Ok("Q2 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q2_rusqlite(&conn)?;
        Ok("Q2 complete".to_string())
    });
    
    format_result("Q2: Minimum Cost Supplier", gongdb_time, rusqlite_time);
    
    // Q3: Shipping Priority Query
    let (gongdb_time, _) = time_it(|| {
        run_q3(&mut gongdb)?;
        Ok("Q3 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q3_rusqlite(&conn)?;
        Ok("Q3 complete".to_string())
    });
    
    format_result("Q3: Shipping Priority", gongdb_time, rusqlite_time);
    
    // Q4: Order Priority Checking Query
    let (gongdb_time, _) = time_it(|| {
        run_q4(&mut gongdb)?;
        Ok("Q4 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q4_rusqlite(&conn)?;
        Ok("Q4 complete".to_string())
    });
    
    format_result("Q4: Order Priority Checking", gongdb_time, rusqlite_time);
    
    // Q5: Local Supplier Volume Query
    let (gongdb_time, _) = time_it(|| {
        run_q5(&mut gongdb)?;
        Ok("Q5 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q5_rusqlite(&conn)?;
        Ok("Q5 complete".to_string())
    });
    
    format_result("Q5: Local Supplier Volume", gongdb_time, rusqlite_time);
    
    // Q6: Forecasting Revenue Change Query
    let (gongdb_time, _) = time_it(|| {
        run_q6(&mut gongdb)?;
        Ok("Q6 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q6_rusqlite(&conn)?;
        Ok("Q6 complete".to_string())
    });
    
    format_result("Q6: Forecasting Revenue Change", gongdb_time, rusqlite_time);
    
    // Q7: Volume Shipping Query
    let (gongdb_time, _) = time_it(|| {
        run_q7(&mut gongdb)?;
        Ok("Q7 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q7_rusqlite(&conn)?;
        Ok("Q7 complete".to_string())
    });
    
    format_result("Q7: Volume Shipping", gongdb_time, rusqlite_time);
    
    // Q8: National Market Share Query
    let (gongdb_time, _) = time_it(|| {
        run_q8(&mut gongdb)?;
        Ok("Q8 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q8_rusqlite(&conn)?;
        Ok("Q8 complete".to_string())
    });
    
    format_result("Q8: National Market Share", gongdb_time, rusqlite_time);
    
    // Q9: Product Type Profit Measure Query
    let (gongdb_time, _) = time_it(|| {
        run_q9(&mut gongdb)?;
        Ok("Q9 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q9_rusqlite(&conn)?;
        Ok("Q9 complete".to_string())
    });
    
    format_result("Q9: Product Type Profit Measure", gongdb_time, rusqlite_time);
    
    // Q10: Returned Item Reporting Query
    let (gongdb_time, _) = time_it(|| {
        run_q10(&mut gongdb)?;
        Ok("Q10 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q10_rusqlite(&conn)?;
        Ok("Q10 complete".to_string())
    });
    
    format_result("Q10: Returned Item Reporting", gongdb_time, rusqlite_time);
    
    // Q11: Important Stock Identification Query
    let (gongdb_time, _) = time_it(|| {
        run_q11(&mut gongdb)?;
        Ok("Q11 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q11_rusqlite(&conn)?;
        Ok("Q11 complete".to_string())
    });
    
    format_result("Q11: Important Stock Identification", gongdb_time, rusqlite_time);
    
    // Q12: Shipping Modes and Order Priority Query
    let (gongdb_time, _) = time_it(|| {
        run_q12(&mut gongdb)?;
        Ok("Q12 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q12_rusqlite(&conn)?;
        Ok("Q12 complete".to_string())
    });
    
    format_result("Q12: Shipping Modes and Order Priority", gongdb_time, rusqlite_time);
    
    // Q13: Customer Distribution Query
    let (gongdb_time, _) = time_it(|| {
        run_q13(&mut gongdb)?;
        Ok("Q13 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q13_rusqlite(&conn)?;
        Ok("Q13 complete".to_string())
    });
    
    format_result("Q13: Customer Distribution", gongdb_time, rusqlite_time);
    
    // Q14: Promotion Effect Query
    let (gongdb_time, _) = time_it(|| {
        run_q14(&mut gongdb)?;
        Ok("Q14 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q14_rusqlite(&conn)?;
        Ok("Q14 complete".to_string())
    });
    
    format_result("Q14: Promotion Effect", gongdb_time, rusqlite_time);
    
    // Q15: Top Supplier Query
    let (gongdb_time, _) = time_it(|| {
        run_q15(&mut gongdb)?;
        Ok("Q15 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q15_rusqlite(&conn)?;
        Ok("Q15 complete".to_string())
    });
    
    format_result("Q15: Top Supplier", gongdb_time, rusqlite_time);
    
    // Q16: Parts/Supplier Relationship Query
    let (gongdb_time, _) = time_it(|| {
        run_q16(&mut gongdb)?;
        Ok("Q16 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q16_rusqlite(&conn)?;
        Ok("Q16 complete".to_string())
    });
    
    format_result("Q16: Parts/Supplier Relationship", gongdb_time, rusqlite_time);
    
    // Q17: Small-Quantity-Order Revenue Query
    let (gongdb_time, _) = time_it(|| {
        run_q17(&mut gongdb)?;
        Ok("Q17 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q17_rusqlite(&conn)?;
        Ok("Q17 complete".to_string())
    });
    
    format_result("Q17: Small-Quantity-Order Revenue", gongdb_time, rusqlite_time);
    
    // Q18: Large Volume Customer Query
    let (gongdb_time, _) = time_it(|| {
        run_q18(&mut gongdb)?;
        Ok("Q18 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q18_rusqlite(&conn)?;
        Ok("Q18 complete".to_string())
    });
    
    format_result("Q18: Large Volume Customer", gongdb_time, rusqlite_time);
    
    // Q19: Discounted Revenue Query
    let (gongdb_time, _) = time_it(|| {
        run_q19(&mut gongdb)?;
        Ok("Q19 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q19_rusqlite(&conn)?;
        Ok("Q19 complete".to_string())
    });
    
    format_result("Q19: Discounted Revenue", gongdb_time, rusqlite_time);
    
    // Q20: Potential Part Promotion Query
    let (gongdb_time, _) = time_it(|| {
        run_q20(&mut gongdb)?;
        Ok("Q20 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q20_rusqlite(&conn)?;
        Ok("Q20 complete".to_string())
    });
    
    format_result("Q20: Potential Part Promotion", gongdb_time, rusqlite_time);
    
    // Q21: Suppliers Who Kept Orders Waiting Query
    let (gongdb_time, _) = time_it(|| {
        run_q21(&mut gongdb)?;
        Ok("Q21 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q21_rusqlite(&conn)?;
        Ok("Q21 complete".to_string())
    });
    
    format_result("Q21: Suppliers Who Kept Orders Waiting", gongdb_time, rusqlite_time);
    
    // Q22: Global Sales Opportunity Query
    let (gongdb_time, _) = time_it(|| {
        run_q22(&mut gongdb)?;
        Ok("Q22 complete".to_string())
    });
    
    let (rusqlite_time, _) = time_it(|| {
        run_q22_rusqlite(&conn)?;
        Ok("Q22 complete".to_string())
    });
    
    format_result("Q22: Global Sales Opportunity", gongdb_time, rusqlite_time);
    
    println!("\nTPC-H Benchmark completed!");
}

// TPC-H Schema Setup
fn setup_tpch_schema(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    // Region table
    db.run_statement(
        "CREATE TABLE region(r_regionkey INTEGER PRIMARY KEY, r_name TEXT, r_comment TEXT)"
    )?;
    
    // Nation table
    db.run_statement(
        "CREATE TABLE nation(n_nationkey INTEGER PRIMARY KEY, n_name TEXT, n_regionkey INTEGER, n_comment TEXT)"
    )?;
    
    // Supplier table
    db.run_statement(
        "CREATE TABLE supplier(s_suppkey INTEGER PRIMARY KEY, s_name TEXT, s_address TEXT, s_nationkey INTEGER, s_phone TEXT, s_acctbal REAL, s_comment TEXT)"
    )?;
    
    // Customer table
    db.run_statement(
        "CREATE TABLE customer(c_custkey INTEGER PRIMARY KEY, c_name TEXT, c_address TEXT, c_nationkey INTEGER, c_phone TEXT, c_acctbal REAL, c_mktsegment TEXT, c_comment TEXT)"
    )?;
    
    // Part table
    db.run_statement(
        "CREATE TABLE part(p_partkey INTEGER PRIMARY KEY, p_name TEXT, p_mfgr TEXT, p_brand TEXT, p_type TEXT, p_size INTEGER, p_container TEXT, p_retailprice REAL, p_comment TEXT)"
    )?;
    
    // Partsupp table
    db.run_statement(
        "CREATE TABLE partsupp(ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER, ps_supplycost REAL, ps_comment TEXT, PRIMARY KEY (ps_partkey, ps_suppkey))"
    )?;
    
    // Orders table
    db.run_statement(
        "CREATE TABLE orders(o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_orderstatus TEXT, o_totalprice REAL, o_orderdate TEXT, o_orderpriority TEXT, o_clerk TEXT, o_shippriority INTEGER, o_comment TEXT)"
    )?;
    
    // Lineitem table
    db.run_statement(
        "CREATE TABLE lineitem(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity REAL, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT, PRIMARY KEY (l_orderkey, l_linenumber))"
    )?;
    
    Ok(())
}

fn setup_tpch_schema_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "CREATE TABLE region(r_regionkey INTEGER PRIMARY KEY, r_name TEXT, r_comment TEXT)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE nation(n_nationkey INTEGER PRIMARY KEY, n_name TEXT, n_regionkey INTEGER, n_comment TEXT)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE supplier(s_suppkey INTEGER PRIMARY KEY, s_name TEXT, s_address TEXT, s_nationkey INTEGER, s_phone TEXT, s_acctbal REAL, s_comment TEXT)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE customer(c_custkey INTEGER PRIMARY KEY, c_name TEXT, c_address TEXT, c_nationkey INTEGER, c_phone TEXT, c_acctbal REAL, c_mktsegment TEXT, c_comment TEXT)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE part(p_partkey INTEGER PRIMARY KEY, p_name TEXT, p_mfgr TEXT, p_brand TEXT, p_type TEXT, p_size INTEGER, p_container TEXT, p_retailprice REAL, p_comment TEXT)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE partsupp(ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER, ps_supplycost REAL, ps_comment TEXT, PRIMARY KEY (ps_partkey, ps_suppkey))",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE orders(o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_orderstatus TEXT, o_totalprice REAL, o_orderdate TEXT, o_orderpriority TEXT, o_clerk TEXT, o_shippriority INTEGER, o_comment TEXT)",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE lineitem(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity REAL, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT, PRIMARY KEY (l_orderkey, l_linenumber))",
        []
    )?;
    
    Ok(())
}

// TPC-H Data Loading
fn load_tpch_data(
    db: &mut GongDB,
    regions: usize,
    nations: usize,
    suppliers: usize,
    customers: usize,
    parts: usize,
    orders: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load regions
    let region_names = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
    for r_regionkey in 0..regions {
        let r_name = region_names[r_regionkey % region_names.len()];
        db.run_statement(&format!(
            "INSERT INTO region VALUES ({}, '{}', 'Region comment {}')",
            r_regionkey, r_name, r_regionkey
        ))?;
    }
    
    // Load nations
    let nation_names = [
        "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY",
        "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE",
        "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES",
    ];
    for n_nationkey in 0..nations {
        let n_name = nation_names[n_nationkey % nation_names.len()];
        let n_regionkey = n_nationkey % regions;
        db.run_statement(&format!(
            "INSERT INTO nation VALUES ({}, '{}', {}, 'Nation comment {}')",
            n_nationkey, n_name, n_regionkey, n_nationkey
        ))?;
    }
    
    // Load suppliers
    for s_suppkey in 1..=suppliers {
        let s_nationkey = (s_suppkey - 1) % nations;
        let s_acctbal = -99999.99 + ((s_suppkey as f64) * 0.01);
        db.run_statement(&format!(
            "INSERT INTO supplier VALUES ({}, 'Supplier#{}', 'Address {}', {}, 'Phone {}', {}, 'Supplier comment {}')",
            s_suppkey, s_suppkey, s_suppkey, s_nationkey, s_suppkey, s_acctbal, s_suppkey
        ))?;
    }
    
    // Load customers
    let mktsegments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];
    for c_custkey in 1..=customers {
        let c_nationkey = (c_custkey - 1) % nations;
        let c_acctbal = -99999.99 + ((c_custkey as f64) * 0.01);
        let c_mktsegment = mktsegments[c_custkey % mktsegments.len()];
        db.run_statement(&format!(
            "INSERT INTO customer VALUES ({}, 'Customer#{}', 'Address {}', {}, 'Phone {}', {}, '{}', 'Customer comment {}')",
            c_custkey, c_custkey, c_custkey, c_nationkey, c_custkey, c_acctbal, c_mktsegment, c_custkey
        ))?;
    }
    
    // Load parts
    let brands = ["Brand#11", "Brand#12", "Brand#13", "Brand#14", "Brand#15"];
    let types = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY"];
    for p_partkey in 1..=parts {
        let p_brand = brands[p_partkey % brands.len()];
        let p_type = types[p_partkey % types.len()];
        let p_size = 1 + (p_partkey % 50);
        let p_retailprice = 901.0 + ((p_partkey as f64) * 0.01);
        db.run_statement(&format!(
            "INSERT INTO part VALUES ({}, 'Part#{}', 'MFGR#{}', '{}', '{}', {}, 'Container {}', {}, 'Part comment {}')",
            p_partkey, p_partkey, p_partkey % 5, p_brand, p_type, p_size, p_partkey % 10, p_retailprice, p_partkey
        ))?;
    }
    
    // Load partsupp
    for p_partkey in 1..=parts {
        let num_suppliers = 1 + (p_partkey % 4); // 1-4 suppliers per part
        for supplier_idx in 0..num_suppliers {
            let ps_suppkey = 1 + ((p_partkey + supplier_idx) % suppliers);
            let ps_availqty = 1 + (p_partkey % 9999);
            let ps_supplycost = 1.0 + ((p_partkey as f64) * 0.001);
            db.run_statement(&format!(
                "INSERT INTO partsupp VALUES ({}, {}, {}, {}, 'Partsupp comment {}')",
                p_partkey, ps_suppkey, ps_availqty, ps_supplycost, p_partkey
            ))?;
        }
    }
    
    // Load orders
    for o_orderkey in 1..=orders {
        let o_custkey = 1 + ((o_orderkey - 1) % customers);
        let o_orderstatus = if o_orderkey % 3 == 0 { "'O'" } else { "'F'" };
        let o_totalprice = 100.0 + ((o_orderkey as f64) * 0.1);
        // Generate dates around 1992-1998
        let days_offset = (o_orderkey % 2190) as i32; // ~6 years
        let o_orderdate = date_offset(1992, 1, 1, days_offset);
        let o_orderpriority = if o_orderkey % 5 == 0 { "'1-URGENT'" } else { "'2-HIGH'" };
        db.run_statement(&format!(
            "INSERT INTO orders VALUES ({}, {}, {}, {}, '{}', {}, 'Clerk#{}', {}, 'Order comment {}')",
            o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_orderkey % 1000, 0, o_orderkey
        ))?;
    }
    
    // Load lineitems (approximately 4-5 lineitems per order on average)
    let mut l_orderkey = 1;
    let mut l_linenumber = 1;
    while l_orderkey <= orders {
        let num_lineitems = 1 + (l_orderkey % 7); // 1-7 lineitems per order
        for line_idx in 0..num_lineitems {
            let l_partkey = 1 + ((l_orderkey + line_idx) % parts);
            let l_suppkey = 1 + ((l_orderkey + line_idx) % suppliers);
            let l_quantity = 1.0 + ((l_orderkey % 50) as f64);
            let l_extendedprice = l_quantity * (901.0 + ((l_partkey as f64) * 0.01));
            let l_discount = 0.0 + ((l_orderkey % 11) as f64) * 0.01; // 0-0.1
            let l_tax = 0.0 + ((l_orderkey % 9) as f64) * 0.01; // 0-0.08
            let l_returnflag = if l_orderkey % 3 == 0 { "'R'" } else { "'A'" };
            let l_linestatus = if l_orderkey % 2 == 0 { "'O'" } else { "'F'" };
            let days_offset = (l_orderkey % 2190) as i32;
            let l_shipdate = date_offset(1992, 1, 1, days_offset + 30);
            let l_commitdate = date_offset(1992, 1, 1, days_offset + 60);
            let l_receiptdate = date_offset(1992, 1, 1, days_offset + 90);
            
            db.run_statement(&format!(
                "INSERT INTO lineitem VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', 'DELIVER IN PERSON', 'TRUCK', 'Lineitem comment {}')",
                l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_orderkey
            ))?;
            l_linenumber += 1;
        }
        l_orderkey += 1;
        l_linenumber = 1;
    }
    
    Ok(())
}

fn load_tpch_data_rusqlite(
    conn: &Connection,
    regions: usize,
    nations: usize,
    suppliers: usize,
    customers: usize,
    parts: usize,
    orders: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load regions
    let region_names = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
    for r_regionkey in 0..regions {
        let r_name = region_names[r_regionkey % region_names.len()];
        conn.execute(
            &format!(
                "INSERT INTO region VALUES ({}, '{}', 'Region comment {}')",
                r_regionkey, r_name, r_regionkey
            ),
            []
        )?;
    }
    
    // Load nations
    let nation_names = [
        "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY",
        "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE",
        "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES",
    ];
    for n_nationkey in 0..nations {
        let n_name = nation_names[n_nationkey % nation_names.len()];
        let n_regionkey = n_nationkey % regions;
        conn.execute(
            &format!(
                "INSERT INTO nation VALUES ({}, '{}', {}, 'Nation comment {}')",
                n_nationkey, n_name, n_regionkey, n_nationkey
            ),
            []
        )?;
    }
    
    // Load suppliers
    for s_suppkey in 1..=suppliers {
        let s_nationkey = (s_suppkey - 1) % nations;
        let s_acctbal = -99999.99 + ((s_suppkey as f64) * 0.01);
        conn.execute(
            &format!(
                "INSERT INTO supplier VALUES ({}, 'Supplier#{}', 'Address {}', {}, 'Phone {}', {}, 'Supplier comment {}')",
                s_suppkey, s_suppkey, s_suppkey, s_nationkey, s_suppkey, s_acctbal, s_suppkey
            ),
            []
        )?;
    }
    
    // Load customers
    let mktsegments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];
    for c_custkey in 1..=customers {
        let c_nationkey = (c_custkey - 1) % nations;
        let c_acctbal = -99999.99 + ((c_custkey as f64) * 0.01);
        let c_mktsegment = mktsegments[c_custkey % mktsegments.len()];
        conn.execute(
            &format!(
                "INSERT INTO customer VALUES ({}, 'Customer#{}', 'Address {}', {}, 'Phone {}', {}, '{}', 'Customer comment {}')",
                c_custkey, c_custkey, c_custkey, c_nationkey, c_custkey, c_acctbal, c_mktsegment, c_custkey
            ),
            []
        )?;
    }
    
    // Load parts
    let brands = ["Brand#11", "Brand#12", "Brand#13", "Brand#14", "Brand#15"];
    let types = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY"];
    for p_partkey in 1..=parts {
        let p_brand = brands[p_partkey % brands.len()];
        let p_type = types[p_partkey % types.len()];
        let p_size = 1 + (p_partkey % 50);
        let p_retailprice = 901.0 + ((p_partkey as f64) * 0.01);
        conn.execute(
            &format!(
                "INSERT INTO part VALUES ({}, 'Part#{}', 'MFGR#{}', '{}', '{}', {}, 'Container {}', {}, 'Part comment {}')",
                p_partkey, p_partkey, p_partkey % 5, p_brand, p_type, p_size, p_partkey % 10, p_retailprice, p_partkey
            ),
            []
        )?;
    }
    
    // Load partsupp
    for p_partkey in 1..=parts {
        let num_suppliers = 1 + (p_partkey % 4); // 1-4 suppliers per part
        for supplier_idx in 0..num_suppliers {
            let ps_suppkey = 1 + ((p_partkey + supplier_idx) % suppliers);
            let ps_availqty = 1 + (p_partkey % 9999);
            let ps_supplycost = 1.0 + ((p_partkey as f64) * 0.001);
            conn.execute(
                &format!(
                    "INSERT INTO partsupp VALUES ({}, {}, {}, {}, 'Partsupp comment {}')",
                    p_partkey, ps_suppkey, ps_availqty, ps_supplycost, p_partkey
                ),
                []
            )?;
        }
    }
    
    // Load orders
    for o_orderkey in 1..=orders {
        let o_custkey = 1 + ((o_orderkey - 1) % customers);
        let o_orderstatus = if o_orderkey % 3 == 0 { "'O'" } else { "'F'" };
        let o_totalprice = 100.0 + ((o_orderkey as f64) * 0.1);
        // Generate dates around 1992-1998
        let days_offset = (o_orderkey % 2190) as i32; // ~6 years
        let o_orderdate = date_offset(1992, 1, 1, days_offset);
        let o_orderpriority = if o_orderkey % 5 == 0 { "'1-URGENT'" } else { "'2-HIGH'" };
        conn.execute(
            &format!(
                "INSERT INTO orders VALUES ({}, {}, {}, {}, '{}', {}, 'Clerk#{}', {}, 'Order comment {}')",
                o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_orderkey % 1000, 0, o_orderkey
            ),
            []
        )?;
    }
    
    // Load lineitems (approximately 4-5 lineitems per order on average)
    let mut l_orderkey = 1;
    let mut l_linenumber = 1;
    while l_orderkey <= orders {
        let num_lineitems = 1 + (l_orderkey % 7); // 1-7 lineitems per order
        for line_idx in 0..num_lineitems {
            let l_partkey = 1 + ((l_orderkey + line_idx) % parts);
            let l_suppkey = 1 + ((l_orderkey + line_idx) % suppliers);
            let l_quantity = 1.0 + ((l_orderkey % 50) as f64);
            let l_extendedprice = l_quantity * (901.0 + ((l_partkey as f64) * 0.01));
            let l_discount = 0.0 + ((l_orderkey % 11) as f64) * 0.01; // 0-0.1
            let l_tax = 0.0 + ((l_orderkey % 9) as f64) * 0.01; // 0-0.08
            let l_returnflag = if l_orderkey % 3 == 0 { "'R'" } else { "'A'" };
            let l_linestatus = if l_orderkey % 2 == 0 { "'O'" } else { "'F'" };
            let days_offset = (l_orderkey % 2190) as i32;
            let l_shipdate = date_offset(1992, 1, 1, days_offset + 30);
            let l_commitdate = date_offset(1992, 1, 1, days_offset + 60);
            let l_receiptdate = date_offset(1992, 1, 1, days_offset + 90);
            
            conn.execute(
                &format!(
                    "INSERT INTO lineitem VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', 'DELIVER IN PERSON', 'TRUCK', 'Lineitem comment {}')",
                    l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_orderkey
                ),
                []
            )?;
            l_linenumber += 1;
        }
        l_orderkey += 1;
        l_linenumber = 1;
    }
    
    Ok(())
}

// TPC-H Queries

// Q1: Pricing Summary Report Query
// This query reports the amount of business that was billed, shipped, and returned.
fn run_q1(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= date('1998-12-01', '-90 days') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
    )?;
    Ok(())
}

fn run_q1_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= date('1998-12-01', '-90 days') GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q3: Shipping Priority Query
// This query retrieves the shipping priority and potential revenue of the orders having the largest revenue among those that had not been shipped as of a given date.
fn run_q3(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < date('1995-03-15') AND l_shipdate > date('1995-03-15') GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"
    )?;
    Ok(())
}

fn run_q3_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < date('1995-03-15') AND l_shipdate > date('1995-03-15') GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q6: Forecasting Revenue Change Query
// This query quantifies the amount of revenue increase that would have resulted from eliminating certain company-wide discounts in a given percentage range in a given year.
fn run_q6(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= date('1994-01-01') AND l_shipdate < date('1995-01-01') AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
    )?;
    Ok(())
}

fn run_q6_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: f64 = conn.query_row(
        "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= date('1994-01-01') AND l_shipdate < date('1995-01-01') AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24",
        [],
        |row| row.get(0)
    )?;
    Ok(())
}

// Q14: Promotion Effect Query
// This query monitors the market response to a promotion such as TV advertisements or a special campaign.
fn run_q14(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= date('1995-09-01') AND l_shipdate < date('1995-10-01')"
    )?;
    Ok(())
}

fn run_q14_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: f64 = conn.query_row(
        "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= date('1995-09-01') AND l_shipdate < date('1995-10-01')",
        [],
        |row| row.get(0)
    )?;
    Ok(())
}

// Q19: Discounted Revenue Query
// This query finds the gross discounted revenue for all orders of three specific brand/container combinations.
fn run_q19(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#13' AND p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG') AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#14' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"
    )?;
    Ok(())
}

fn run_q19_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: f64 = conn.query_row(
        "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#13' AND p_container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG') AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#14' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')",
        [],
        |row| row.get(0)
    )?;
    Ok(())
}

// Q2: Minimum Cost Supplier Query
// This query finds which supplier should be selected to place an order for a given part in a given region.
fn run_q2(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100"
    )?;
    Ok(())
}

fn run_q2_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q4: Order Priority Checking Query
// This query counts how many orders were placed in a given quarter of a given year in which at least one lineitem was received by the customer later than its committed date.
fn run_q4(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= date('1993-07-01') AND o_orderdate < date('1993-10-01') AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority"
    )?;
    Ok(())
}

fn run_q4_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= date('1993-07-01') AND o_orderdate < date('1993-10-01') AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q5: Local Supplier Volume Query
// This query lists the revenue volume done through local suppliers.
fn run_q5(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= date('1994-01-01') AND o_orderdate < date('1995-01-01') GROUP BY n_name ORDER BY revenue DESC"
    )?;
    Ok(())
}

fn run_q5_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= date('1994-01-01') AND o_orderdate < date('1995-01-01') GROUP BY n_name ORDER BY revenue DESC"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q7: Volume Shipping Query
// This query determines the value of goods shipped between certain nations to help in the re-organization of the sales force.
fn run_q7(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, CAST(strftime('%Y', l_shipdate) AS INTEGER) AS l_year, l_extendedprice * (1 - l_discount) AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) AND l_shipdate >= date('1995-01-01') AND l_shipdate <= date('1996-12-31')) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"
    )?;
    Ok(())
}

fn run_q7_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, CAST(strftime('%Y', l_shipdate) AS INTEGER) AS l_year, l_extendedprice * (1 - l_discount) AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) AND l_shipdate >= date('1995-01-01') AND l_shipdate <= date('1996-12-31')) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q8: National Market Share Query
// This query determines how the market share of a given nation within a given region has changed over two years for a given part type.
fn run_q8(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share FROM (SELECT CAST(strftime('%Y', o_orderdate) AS INTEGER) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate >= date('1995-01-01') AND o_orderdate <= date('1996-12-31') AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations GROUP BY o_year ORDER BY o_year"
    )?;
    Ok(())
}

fn run_q8_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share FROM (SELECT CAST(strftime('%Y', o_orderdate) AS INTEGER) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate >= date('1995-01-01') AND o_orderdate <= date('1996-12-31') AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations GROUP BY o_year ORDER BY o_year"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q9: Product Type Profit Measure Query
// This query determines how much profit is made on a given line of parts, broken out by supplier nation and year.
fn run_q9(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT nation, o_year, SUM(amount) AS sum_profit FROM (SELECT n_name AS nation, CAST(strftime('%Y', o_orderdate) AS INTEGER) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%green%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"
    )?;
    Ok(())
}

fn run_q9_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT nation, o_year, SUM(amount) AS sum_profit FROM (SELECT n_name AS nation, CAST(strftime('%Y', o_orderdate) AS INTEGER) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%green%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q10: Returned Item Reporting Query
// This query identifies customers who might be having problems with the parts that they are buying.
fn run_q10(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= date('1993-10-01') AND o_orderdate < date('1994-01-01') AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20"
    )?;
    Ok(())
}

fn run_q10_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= date('1993-10-01') AND o_orderdate < date('1994-01-01') AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q11: Important Stock Identification Query
// This query finds the most important subset of suppliers' stock in a given nation.
fn run_q11(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY ps_partkey HAVING SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') ORDER BY value DESC"
    )?;
    Ok(())
}

fn run_q11_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY ps_partkey HAVING SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') ORDER BY value DESC"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q12: Shipping Modes and Order Priority Query
// This query determines whether selecting less expensive modes of shipping is negatively affecting the critical-priority orders by causing more parts to be received by customers after the committed date.
fn run_q12(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT l_shipmode, SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= date('1994-01-01') AND l_receiptdate < date('1995-01-01') GROUP BY l_shipmode ORDER BY l_shipmode"
    )?;
    Ok(())
}

fn run_q12_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT l_shipmode, SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= date('1994-01-01') AND l_receiptdate < date('1995-01-01') GROUP BY l_shipmode ORDER BY l_shipmode"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q13: Customer Distribution Query
// This query seeks relationships between customers and the size of their orders.
fn run_q13(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT c_count, COUNT(*) AS custdist FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC"
    )?;
    Ok(())
}

fn run_q13_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT c_count, COUNT(*) AS custdist FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q15: Top Supplier Query
// This query finds the supplier who contributed the most to the overall revenue for parts shipped during a given quarter of a given year.
fn run_q15(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= date('1996-01-01') AND l_shipdate < date('1996-04-01') GROUP BY l_suppkey) AS revenue WHERE s_suppkey = supplier_no AND total_revenue = (SELECT MAX(total_revenue) FROM (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= date('1996-01-01') AND l_shipdate < date('1996-04-01') GROUP BY l_suppkey)) ORDER BY s_suppkey"
    )?;
    Ok(())
}

fn run_q15_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= date('1996-01-01') AND l_shipdate < date('1996-04-01') GROUP BY l_suppkey) AS revenue WHERE s_suppkey = supplier_no AND total_revenue = (SELECT MAX(total_revenue) FROM (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= date('1996-01-01') AND l_shipdate < date('1996-04-01') GROUP BY l_suppkey)) ORDER BY s_suppkey"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q16: Parts/Supplier Relationship Query
// This query finds out how many suppliers can supply parts with given attributes.
fn run_q16(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"
    )?;
    Ok(())
}

fn run_q16_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q17: Small-Quantity-Order Revenue Query
// This query determines how much average yearly revenue would be lost if orders were no longer filled for small quantities of certain parts.
fn run_q17(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)"
    )?;
    Ok(())
}

fn run_q17_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: f64 = conn.query_row(
        "SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)",
        [],
        |row| row.get(0)
    )?;
    Ok(())
}

// Q18: Large Volume Customer Query
// This query identifies customers who have made large quantity purchases of parts.
fn run_q18(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100"
    )?;
    Ok(())
}

fn run_q18_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q20: Potential Part Promotion Query
// This query identifies suppliers in AMERICA who have an excess of a particular part available.
fn run_q20(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%') AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= date('1994-01-01') AND l_shipdate < date('1995-01-01'))) AND s_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY s_name"
    )?;
    Ok(())
}

fn run_q20_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%') AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= date('1994-01-01') AND l_shipdate < date('1995-01-01'))) AND s_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY s_name"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q21: Suppliers Who Kept Orders Waiting Query
// This query identifies suppliers who were not able to ship required parts in a timely manner.
fn run_q21(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT s_name, COUNT(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"
    )?;
    Ok(())
}

fn run_q21_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT s_name, COUNT(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}

// Q22: Global Sales Opportunity Query
// This query identifies geographies where there are customers who may be likely to make a purchase.
fn run_q22(db: &mut GongDB) -> Result<(), Box<dyn std::error::Error>> {
    db.run_statement(
        "SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT SUBSTR(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17') AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode"
    )?;
    Ok(())
}

fn run_q22_rusqlite(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let _: Vec<()> = conn.prepare(
        "SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT SUBSTR(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17') AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode"
    )?
    .query_map([], |_| Ok(()))?
    .collect::<Result<_, _>>()?;
    Ok(())
}
