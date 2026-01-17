# SQLite Test Files

This directory contains test files downloaded from the SQLite sqllogictest repository.

## Source

These test files were downloaded from:
- https://www.sqlite.org/sqllogictest/

To download or update these files, use the `download_all_tests.sh` script in the project root.

## Files

### Main Test Files
- `select1.test` - Basic SELECT queries
- `select2.test` - More SELECT queries
- `select3.test` - Advanced SELECT queries
- `select4.test` - Complex SELECT queries
- `select5.test` - Additional SELECT queries

### Evidence Directory
The `evidence/` subdirectory contains language feature tests:
- `in1.test`, `in2.test` - IN clause tests
- `slt_lang_aggfunc.test` - Aggregate function tests
- `slt_lang_createtrigger.test` - CREATE TRIGGER tests
- `slt_lang_createview.test` - CREATE VIEW tests
- `slt_lang_dropindex.test` - DROP INDEX tests
- `slt_lang_droptable.test` - DROP TABLE tests
- `slt_lang_droptrigger.test` - DROP TRIGGER tests
- `slt_lang_dropview.test` - DROP VIEW tests
- `slt_lang_reindex.test` - REINDEX tests
- `slt_lang_replace.test` - REPLACE statement tests
- `slt_lang_update.test` - UPDATE statement tests

### Random Test Suites

The `random/` subdirectory contains comprehensive randomized test suites:

- **random/aggregates/**: 130 files testing aggregate functions
- **random/expr/**: 120 files testing SQL expressions
- **random/groupby/**: 14 files testing GROUP BY functionality
- **random/select/**: 127 files testing SELECT queries

## Total

- **408 test files**
- **654MB of test data**
- **Millions of test cases**

## Running the Tests

All test files in this directory are automatically discovered and run by the test runner:

```bash
cargo run --bin run_tests
```

The test runner recursively scans this directory and runs all `.test` files in sorted order. This includes all 408 test files across all subdirectories.

**Note**: Running all 408 test files will take significant time. The test runner uses SQLite as the backend for execution.

## Format

These files use the `.test` extension but are compatible with the sqllogictest format (same as `.slt` files). Many tests use hash-based result checking for efficiency when dealing with large result sets.
