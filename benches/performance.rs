//! Criterion benchmarks for gongdb performance analysis.
//!
//! Run with: `cargo bench --bench performance`
//!
//! Results are saved to `target/criterion/performance/` with HTML reports.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gongdb::engine::GongDB;

fn bench_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut db = GongDB::new_in_memory().unwrap();
                    db.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
                    
                    for i in 0..size {
                        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i * 2);
                        db.run_statement(black_box(&sql)).unwrap();
                    }
                });
            },
        );
    }
    
    group.finish();
}

fn bench_select_where(c: &mut Criterion) {
    let mut db = GongDB::new_in_memory().unwrap();
    db.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    
    // Insert test data
    for i in 0..10000 {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        db.run_statement(&sql).unwrap();
    }
    
    c.bench_function("select_where", |b| {
        b.iter(|| {
            db.run_statement(black_box("SELECT * FROM t1 WHERE val = 50")).unwrap();
        });
    });
}

fn bench_aggregation_count(c: &mut Criterion) {
    let mut db = GongDB::new_in_memory().unwrap();
    db.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    
    // Insert test data
    for i in 0..10000 {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        db.run_statement(&sql).unwrap();
    }
    
    c.bench_function("aggregation_count", |b| {
        b.iter(|| {
            db.run_statement(black_box("SELECT COUNT(*) FROM t1")).unwrap();
        });
    });
}

fn bench_aggregation_sum(c: &mut Criterion) {
    let mut db = GongDB::new_in_memory().unwrap();
    db.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    
    // Insert test data
    for i in 0..10000 {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        db.run_statement(&sql).unwrap();
    }
    
    c.bench_function("aggregation_sum", |b| {
        b.iter(|| {
            db.run_statement(black_box("SELECT SUM(val) FROM t1")).unwrap();
        });
    });
}

fn bench_group_by(c: &mut Criterion) {
    let mut db = GongDB::new_in_memory().unwrap();
    db.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    
    // Insert test data
    for i in 0..10000 {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, i % 100);
        db.run_statement(&sql).unwrap();
    }
    
    c.bench_function("group_by", |b| {
        b.iter(|| {
            db.run_statement(black_box("SELECT val, COUNT(*) FROM t1 GROUP BY val")).unwrap();
        });
    });
}

fn bench_order_by(c: &mut Criterion) {
    let mut db = GongDB::new_in_memory().unwrap();
    db.run_statement("CREATE TABLE t1(id INTEGER, val INTEGER)").unwrap();
    
    // Insert test data
    for i in 0..10000 {
        let sql = format!("INSERT INTO t1 VALUES ({}, {})", i, (i * 7) % 1000);
        db.run_statement(&sql).unwrap();
    }
    
    c.bench_function("order_by", |b| {
        b.iter(|| {
            db.run_statement(black_box("SELECT * FROM t1 ORDER BY val LIMIT 100")).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_bulk_insert,
    bench_select_where,
    bench_aggregation_count,
    bench_aggregation_sum,
    bench_group_by,
    bench_order_by
);
criterion_main!(benches);
