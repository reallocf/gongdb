# Custom Test Suite

This directory contains custom tests for phases that aren't sufficiently covered by the SQLite test suite.

## Test Files

### `phase2_storage_engine.test`
Tests for the storage engine implementation:
- Page-based storage
- Data persistence
- Row serialization with various data types
- Variable-length data handling
- Large value storage
- Storage cleanup on DROP

### `phase3_type_system.test`
Tests for the type system:
- Type coercion and affinity
- Implicit type conversions
- NULL handling in comparisons and arithmetic
- Type preservation in aggregates
- Mixed type operations

### `phase11_indexing.test`
Tests for indexing:
- Index creation and usage
- Multi-column indexes
- Index maintenance on UPDATE/DELETE
- NULL handling in indexes
- Index with large datasets
- ORDER BY optimization with indexes

### `phase12_query_planning.test`
Tests for query planning and optimization:
- Index scan vs table scan selection
- Join order optimization
- Predicate pushdown
- Cost estimation
- Subquery optimization
- Aggregate optimization

### `phase13_transactions.test`
Tests for transaction management:
- BEGIN/COMMIT/ROLLBACK
- Atomicity (all-or-nothing)
- Consistency (constraint enforcement)
- Isolation (transaction boundaries)
- Durability (data persistence)
- Nested transactions (if supported)
- DDL in transactions

## Running Custom Tests

To run all custom tests:
```bash
cargo test --test sqllogictest test_custom -- --no-capture
```

To run a specific custom test:
```bash
cargo test --test sqllogictest test_phase2_storage_engine -- --no-capture
```

## Integration

These tests are integrated into the main test suite and will be run as part of the comprehensive test validation for each phase.
