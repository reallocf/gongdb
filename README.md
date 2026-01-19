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
   cargo test -- --no-capture
   ```

## Dependencies

### Runtime Dependencies

- `sqllogictest`: SQL logic test framework for testing database implementations (v0.29)
- `rusqlite`: SQLite database driver for running tests
- `tokio`: Async runtime
- `async-trait`: Async trait support

### Development Dependencies

- `dhat`: Heap profiling tool for memory analysis
- `criterion`: Statistical benchmarking framework with HTML reports

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

## Performance Profiling

gongdb includes comprehensive profiling tools for performance analysis and optimization.

### Available Tools

1. **CPU Profiling**: Flamegraphs using `cargo-flamegraph`
2. **macOS Instruments**: Native profiling via `cargo-instruments` (macOS only)
3. **Heap Profiling**: Memory allocation tracking with `dhat-rs`
4. **Benchmarking**: Statistical benchmarks with `criterion`

### Quick Start

```bash
# Install profiling tools (one-time setup)
cargo install flamegraph
cargo install cargo-instruments  # macOS only

# Generate CPU flamegraph
cargo flamegraph --release --bin gongdb

# Run benchmarks
cargo bench --bench performance

# Profile performance tests
cargo flamegraph --test performance
```

### Documentation

For detailed profiling instructions, examples, and best practices, see [`docs/PROFILING.md`](docs/PROFILING.md).

The profiling guide covers:
- Setting up profiling tools
- CPU profiling with flamegraphs
- macOS Instruments integration
- Heap profiling with dhat-rs
- Structured benchmarking with criterion
- Performance analysis workflows
- Troubleshooting common issues

## Project Structure

```
gongdb/
├── Cargo.toml              # Project manifest
├── src/
│   ├── main.rs            # Main entry point
│   └── bin/
│       └── run_tests.rs   # SQL logic test runner
├── docs/
│   ├── ARCHITECTURE.md    # Architecture and design documentation
│   └── PROFILING.md       # Performance profiling guide
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
