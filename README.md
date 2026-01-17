# GongDB

A database project built with Rust, including comprehensive SQL test coverage using sqllogictest.

## Development Setup

### Prerequisites

- Rust toolchain (install from [rustup.rs](https://rustup.rs/))

### Getting Started

1. Install Rust (if not already installed):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. Build the project:
   ```bash
   cargo build
   ```

3. Run the project:
   ```bash
   cargo run
   ```

4. Run tests:
   ```bash
   cargo test
   ```

## Dependencies

- `sqllogictest`: SQL logic test framework for testing database implementations (v0.29)
- `rusqlite`: SQLite database driver for running tests
- `tokio`: Async runtime
- `async-trait`: Async trait support

## SQL Logic Testing

This project includes `sqllogictest` for testing SQL implementations. Test files use the `.slt` and `.test` formats.

### Running SQL Logic Tests

Run all SQL logic tests using the built-in test runner:

```bash
cargo run --bin run_tests
```

This will execute all SQLite test files from the official SQLite sqllogictest repository.

**Note**: Running all 408 test files will take significant time. The test runner uses SQLite as the backend for execution.

### SQLite Test Suite

This project includes the complete SQLite sqllogictest test suite:

- **408 test files** (654MB total)
- **Source**: https://www.sqlite.org/sqllogictest/
- **Test categories**:
  - Main tests: `select1.test` through `select5.test` (5 files)
  - Evidence tests: Language feature tests (12 files)
  - Random/aggregates: Aggregate function tests (130 files)
  - Random/expr: Expression tests (120 files)
  - Random/groupby: GROUP BY tests (14 files)
  - Random/select: SELECT query tests (127 files)

All test files are located in `tests/sqlite/` with the same directory structure as the source repository.

See `tests/sqlite/README.md` for more details about the test files.

## Project Structure

```
gongdb/
├── Cargo.toml              # Project manifest
├── src/
│   ├── main.rs            # Main entry point
│   └── bin/
│       └── run_tests.rs   # SQL logic test runner
├── tests/
│   └── sqlite/            # SQLite test suite (408 files)
│       ├── *.test         # Main test files
│       ├── evidence/      # Language feature tests
│       └── random/         # Random test suites
│           ├── aggregates/
│           ├── expr/
│           ├── groupby/
│           └── select/
└── README.md              # This file
```
