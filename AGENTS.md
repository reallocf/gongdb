# AGENTS.md - How to Compile and Run Tests

This document provides instructions for AI agents on how to compile and run tests for the GongDB project.

## Prerequisites

- Rust toolchain must be installed (via rustup)
- Cargo must be available in the PATH (may need to source `~/.cargo/env`)

## Compiling the Project

To compile the project:

```bash
# Ensure cargo is in PATH (if needed)
source ~/.cargo/env

# Build the project
cargo build
```

This will compile the project and create binaries in `target/debug/`.

## Running Tests

There are multiple ways to run tests depending on what you want to test:

### 1. Run Unit Tests

To run unit tests (currently none defined in the library):

```bash
cargo test --lib
```

### 2. Run Integration Tests

The project has integration tests in `tests/sqllogictest.rs`. To run them:

```bash
# Run the example test (if example.slt exists)
cargo test --test sqllogictest test_example_slt

# Run all integration tests (excluding ignored ones)
cargo test --test sqllogictest
```

### 3. Run All SQLite Tests (Including Ignored)

The full SQLite test suite is marked as `#[ignore]` because it takes a long time. To run it:

```bash
cargo test --test sqllogictest -- --ignored
```

This will run all 408 SQLite test files, which may take significant time.

### 4. Run Tests Using the Test Binary

The project includes a dedicated test runner binary (`run_tests`). To use it:

```bash
cargo run --bin run_tests
```

This binary:
- Runs `tests/example.slt` (if it exists)
- Discovers and runs all `.test` files in `tests/sqlite/` recursively
- Reports which tests pass or fail
- Exits with code 1 if any tests fail

### 5. Run a Specific Test File

To run a specific test file, you can use the test binary with a custom command or modify the code. The `run_test_file` function in `src/lib.rs` can be used programmatically.

## Test Structure

- **Unit tests**: Located in `src/` files (currently none)
- **Integration tests**: Located in `tests/sqllogictest.rs`
- **Test files**: SQL logic test files (`.test` and `.slt` formats) in `tests/sqlite/`

## Test Files

The project includes:
- 408 SQLite test files in `tests/sqlite/`
- Test categories:
  - Main tests: `select1.test` through `select5.test`
  - Evidence tests: Language feature tests
  - Random tests: Aggregates, expressions, groupby, and select queries

## Notes

- Some test files may fail due to parsing errors with certain SQLite-specific directives (e.g., `onlyif`, `skipif`)
- The test runner uses SQLite as the backend for execution
- Running all 408 test files will take significant time
- The project uses `sqllogictest` v0.29 for test execution

## Quick Reference

```bash
# Compile
cargo build

# Run quick tests
cargo test --test sqllogictest

# Run all tests (long-running)
cargo test --test sqllogictest -- --ignored

# Run via binary
cargo run --bin run_tests
```
