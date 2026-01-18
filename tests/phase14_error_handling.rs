use gongdb::engine::GongDB;
use rusqlite::Connection;

fn gongdb_error(statements: &[&str]) -> String {
    let mut db = GongDB::new_in_memory().expect("create gongdb");
    for sql in statements {
        if let Err(err) = db.run_statement(sql) {
            return err.to_string();
        }
    }
    panic!("expected gongdb error");
}

fn sqlite_error(statements: &[&str]) -> String {
    let conn = Connection::open_in_memory().expect("create sqlite");
    for sql in statements {
        if let Err(err) = conn.execute(sql, []) {
            return err.to_string();
        }
    }
    panic!("expected sqlite error");
}

#[test]
fn syntax_error_matches_sqlite() {
    let statements = ["SELEC 1"]; // misspelled SELECT
    let gong_err = gongdb_error(&statements);
    let sqlite_err = sqlite_error(&statements);
    assert_eq!(gong_err, sqlite_err);
}

#[test]
fn unique_constraint_error_matches_sqlite() {
    let statements = [
        "CREATE TABLE t1(a INTEGER UNIQUE)",
        "INSERT INTO t1 VALUES (1)",
        "INSERT INTO t1 VALUES (1)",
    ];
    let gong_err = gongdb_error(&statements);
    let sqlite_err = sqlite_error(&statements);
    assert_eq!(gong_err, sqlite_err);
}

#[test]
fn not_null_constraint_error_matches_sqlite() {
    let statements = [
        "CREATE TABLE t2(a INTEGER NOT NULL)",
        "INSERT INTO t2 VALUES (NULL)",
    ];
    let gong_err = gongdb_error(&statements);
    let sqlite_err = sqlite_error(&statements);
    assert_eq!(gong_err, sqlite_err);
}
