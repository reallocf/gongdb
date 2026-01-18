use gongdb::engine::GongDB;
use sqllogictest::DBOutput;
use sqllogictest::DefaultColumnType;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_db_path(label: &str) -> String {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("gongdb_phase13_{}_{}.db", label, nanos));
    path.to_string_lossy().to_string()
}

fn count_from_output(output: DBOutput<DefaultColumnType>) -> i64 {
    match output {
        DBOutput::Rows { rows, .. } => rows
            .get(0)
            .and_then(|row| row.get(0))
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0),
        _ => panic!("expected row output"),
    }
}

#[test]
fn write_lock_blocks_read_committed_allows_read_uncommitted() {
    let path = temp_db_path("write_lock");
    let mut writer = GongDB::new_on_disk(&path).expect("open writer db");

    writer
        .run_statement("CREATE TABLE lock_test (id INTEGER)")
        .expect("create table");
    writer
        .run_statement("INSERT INTO lock_test VALUES (1)")
        .expect("seed data");

    writer
        .run_statement("BEGIN TRANSACTION")
        .expect("begin write txn");
    writer
        .run_statement("INSERT INTO lock_test VALUES (2)")
        .expect("insert under write lock");

    let mut reader = GongDB::new_on_disk(&path).expect("open reader db");
    let locked = reader.run_statement("SELECT COUNT(*) FROM lock_test");
    assert!(
        locked.is_err(),
        "read committed should block on writer lock"
    );

    reader
        .run_statement("BEGIN TRANSACTION READ UNCOMMITTED")
        .expect("begin read uncommitted");
    let count = count_from_output(
        reader
            .run_statement("SELECT COUNT(*) FROM lock_test")
            .expect("read uncommitted should see data"),
    );
    assert_eq!(count, 2, "read uncommitted should see uncommitted row");

    reader
        .run_statement("ROLLBACK")
        .expect("rollback read txn");
    writer
        .run_statement("ROLLBACK")
        .expect("rollback writer txn");
}

#[test]
fn shared_reads_block_writer() {
    let path = temp_db_path("read_lock");
    let mut reader = GongDB::new_on_disk(&path).expect("open reader db");
    reader
        .run_statement("CREATE TABLE lock_read (id INTEGER)")
        .expect("create table");
    reader
        .run_statement("INSERT INTO lock_read VALUES (1)")
        .expect("seed data");

    reader
        .run_statement("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .expect("begin serializable read txn");
    let count = count_from_output(
        reader
            .run_statement("SELECT COUNT(*) FROM lock_read")
            .expect("read under serializable"),
    );
    assert_eq!(count, 1);

    let mut writer = GongDB::new_on_disk(&path).expect("open writer db");
    writer
        .run_statement("BEGIN TRANSACTION")
        .expect("begin writer txn");
    let write_attempt = writer.run_statement("INSERT INTO lock_read VALUES (2)");
    assert!(
        write_attempt.is_err(),
        "writer should be blocked by shared read lock"
    );

    writer
        .run_statement("ROLLBACK")
        .expect("rollback writer txn");
    reader
        .run_statement("ROLLBACK")
        .expect("rollback reader txn");
}
