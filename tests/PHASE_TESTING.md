# Phase Testing Guide

This document explains how to run tests for each phase of the database implementation.

## Quick Reference

Run tests for a specific phase:
```bash
cargo test --test sqllogictest test_phase_<N>
```

Where `<N>` is the phase number (1-14).

## Phase Test Commands

### Phase 1: Core Infrastructure (Parser/AST)
```bash
cargo test --test sqllogictest test_phase_1
```
**Tests**: Basic parsing from `select1.test` (CREATE, INSERT, SELECT)

### Phase 2: Storage Engine
```bash
cargo test --test sqllogictest test_phase_2
```
**Tests**: 
- `tests/custom/phase2_storage_engine.test` - Storage engine validation
- `tests/sqlite/select1.test` - Data persistence verification

### Phase 3: Type System
```bash
cargo test --test sqllogictest test_phase_3
```
**Tests**:
- `tests/custom/phase3_type_system.test` - Type coercion and NULL handling
- `tests/sqlite/select1.test` - Mixed data types

### Phase 4: DDL Implementation
```bash
cargo test --test sqllogictest test_phase_4
```
**Tests**: CREATE/DROP TABLE, INDEX, VIEW from `tests/sqlite/evidence/`

### Phase 5: Expression Evaluation
```bash
# Quick test (first 10 expression tests)
cargo test --test sqllogictest test_phase_5

# All expression tests (120+ files)
cargo test --test sqllogictest test_phase_5_all -- --ignored
```
**Tests**: 
- `tests/sqlite/random/expr/*` - Expression tests
- `tests/sqlite/evidence/in1.test`, `in2.test` - IN operator
- `tests/sqlite/index/between/*` - BETWEEN operator

### Phase 6: DML Implementation
```bash
cargo test --test sqllogictest test_phase_6
```
**Tests**:
- `tests/sqlite/evidence/slt_lang_update.test` - UPDATE
- `tests/sqlite/evidence/slt_lang_replace.test` - REPLACE
- `tests/sqlite/index/delete/*` - DELETE
- `tests/sqlite/select1.test` - INSERT

### Phase 7: Query Execution
```bash
cargo test --test sqllogictest test_phase_7
```
**Tests**:
- `tests/sqlite/select1.test` - Basic queries
- `tests/sqlite/select2.test` - More complex queries

### Phase 8: Aggregation
```bash
cargo test --test sqllogictest test_phase_8
```
**Tests**:
- `tests/sqlite/random/aggregates/*` - Aggregate functions (130 files)
- `tests/sqlite/random/groupby/*` - GROUP BY (14 files)
- `tests/sqlite/evidence/slt_lang_aggfunc.test` - Aggregate function tests

### Phase 9: Joins
```bash
cargo test --test sqllogictest test_phase_9
```
**Tests**:
- `tests/sqlite/select2.test` through `select5.test` - Join queries

### Phase 10: Subqueries
```bash
cargo test --test sqllogictest test_phase_10
```
**Tests**:
- `tests/sqlite/evidence/in1.test`, `in2.test` - IN subqueries
- `tests/sqlite/select1.test`, `select2.test` - Scalar and EXISTS subqueries

### Phase 11: Indexing
```bash
cargo test --test sqllogictest test_phase_11
```
**Tests**:
- `tests/custom/phase11_indexing.test` - Index structure and maintenance
- `tests/sqlite/index/*` - Index usage tests
- `tests/sqlite/evidence/slt_lang_dropindex.test`, `slt_lang_reindex.test`

### Phase 12: Query Planning
```bash
cargo test --test sqllogictest test_phase_12
```
**Tests**:
- `tests/custom/phase12_query_planning.test` - Query planner validation
- `tests/sqlite/select1.test`, `select2.test` - Plan selection

### Phase 13: Transaction Management
```bash
cargo test --test sqllogictest test_phase_13
```
**Tests**:
- `tests/custom/phase13_transactions.test` - ACID properties, transactions

### Phase 14: Integration
```bash
cargo test --test sqllogictest test_phase_14
```
**Tests**:
- `tests/sqlite/select1.test` - End-to-end integration

## Running Multiple Phases

You can run multiple phases in sequence:
```bash
cargo test --test sqllogictest test_phase_1 test_phase_2 test_phase_3
```

## Running All Custom Tests

```bash
cargo test --test sqllogictest test_all_custom_files
```

## Running All SQLite Tests

```bash
cargo test --test sqllogictest test_all_sqlite_files -- --ignored
```

## Individual Test Files

You can also run individual test files:
```bash
cargo test --test sqllogictest test_select1
cargo test --test sqllogictest test_phase2_storage_engine
```

## Notes

- Phase tests that collect many files (like Phase 5, 8, 11) run a subset by default for speed
- Use the `--ignored` flag to run full test suites (e.g., `test_phase_5_all`)
- Phase tests provide progress output showing which files are being tested
- Failed tests will show which specific test file failed
