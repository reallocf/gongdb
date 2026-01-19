# Performance Profiling Guide

This guide describes the profiling tools available for analyzing gongdb's performance.

## Overview

gongdb includes several profiling tools to help identify performance bottlenecks:

1. **CPU Profiling**: Flamegraphs using `cargo-flamegraph`
2. **macOS Instruments**: Native macOS profiling via `cargo-instruments`
3. **Heap Profiling**: Memory allocation tracking with `dhat-rs`
4. **Benchmarking**: Structured benchmarks with `criterion`

## Prerequisites

### Install Cargo Plugins

```bash
# Install flamegraph tool (works on macOS, Linux, Windows)
cargo install flamegraph

# Install Instruments tool (macOS only)
cargo install cargo-instruments
```

**Note**: On macOS, you may need to disable System Integrity Protection (SIP) for dtrace to work with flamegraph. Alternatively, use `cargo-instruments` which doesn't require SIP changes.

### System Requirements

- **macOS**: Requires Xcode Command Line Tools for Instruments
- **Linux**: Requires `perf` (usually pre-installed)
- **Windows**: Limited support; consider using WSL2

## CPU Profiling with Flamegraphs

Flamegraphs provide a visual representation of where your program spends time.

### Basic Usage

```bash
# Profile a release build
cargo flamegraph --bin gongdb

# Profile with specific arguments
cargo flamegraph --bin gongdb -- --your-args

# Profile tests
cargo flamegraph --test performance

# Profile with optimizations
RUSTFLAGS="-C force-frame-pointers=yes" cargo flamegraph --release --bin gongdb
```

### Advanced Options

```bash
# Profile only specific functions (regex)
cargo flamegraph --open -- --filter "gongdb::"

# Profile with call-graph depth
cargo flamegraph --depth 20 --bin gongdb

# Save output to specific file
cargo flamegraph -o my-profile.svg --bin gongdb
```

### Interpreting Flamegraphs

- **Width**: Time spent in function (wider = more time)
- **Height**: Call stack depth
- **Colors**: Random for visual distinction
- **Click**: Zoom into specific functions
- **Search**: Use Ctrl+F to find specific functions

## macOS Instruments Profiling

`cargo-instruments` integrates with Apple's Instruments.app for comprehensive profiling.

### Available Templates

```bash
# List available templates
cargo instruments --list

# Time Profiler (CPU usage)
cargo instruments -t "Time Profiler" --bin gongdb

# Allocations (memory tracking)
cargo instruments -t "Allocations" --bin gongdb

# Leaks (memory leak detection)
cargo instruments -t "Leaks" --bin gongdb

# System Trace (comprehensive)
cargo instruments -t "System Trace" --bin gongdb
```

### Example Workflow

```bash
# Profile with Time Profiler
cargo instruments -t "Time Profiler" --release --bin gongdb

# Profile tests
cargo instruments -t "Time Profiler" --test performance

# Open results automatically
cargo instruments -t "Time Profiler" --open --bin gongdb
```

### Instruments Features

- **Time Profiler**: Shows CPU usage over time with call trees
- **Allocations**: Tracks memory allocations and deallocations
- **Leaks**: Detects memory leaks
- **System Trace**: Comprehensive system-level profiling
- **Energy Log**: Power consumption (useful for mobile)

## Heap Profiling with dhat-rs

`dhat-rs` tracks heap allocations to identify memory hotspots.

### Basic Usage

Add to your code temporarily:

```rust
#[cfg(test)]
mod tests {
    use dhat::{Dhat, DhatAlloc};

    #[global_allocator]
    static ALLOCATOR: DhatAlloc = DhatAlloc;

    #[test]
    fn test_with_heap_profiling() {
        let _dhat = Dhat::start_heap_profiling();
        
        // Your code here
        let mut db = GongDB::new_in_memory().unwrap();
        // ... operations ...
        
        // dhat will print stats when _dhat is dropped
    }
}
```

### Running with dhat

```bash
# Run test with heap profiling
cargo test --test performance test_bulk_insert -- --nocapture

# The output will show:
# - Total bytes allocated
# - Peak heap size
# - Allocation sites
```

### dhat Output Example

```
dhat: Total:     1,234,567 bytes in 5,432 blocks
dhat: At t-gmax: 987,654 bytes in 3,210 blocks
dhat: At t-end:  123,456 bytes in 456 blocks
dhat: The data has been saved to dhat-heap.json
```

## Benchmarking with Criterion

Criterion provides statistical benchmarking with HTML reports.

### Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench performance

# Run with specific filter
cargo bench --bench performance -- test_bulk_insert
```

### Benchmark Output

Criterion generates:
- **Console output**: Statistical analysis of benchmark results
- **HTML reports**: Detailed graphs and comparisons in `target/criterion/`
- **Baseline comparisons**: Tracks performance over time

### Viewing Results

```bash
# Open HTML report (after running benchmarks)
open target/criterion/performance/report/index.html
```

## Performance Test Profiling

The existing performance tests in `tests/performance.rs` can be profiled:

```bash
# Profile performance tests with flamegraph
cargo flamegraph --test performance

# Profile with Instruments
cargo instruments -t "Time Profiler" --test performance

# Run performance tests normally
cargo test --test performance -- --nocapture
```

## Profiling Workflow

### 1. Identify Slow Operations

```bash
# Run performance tests to see which operations are slow
cargo test --test performance -- --nocapture
```

### 2. Generate CPU Profile

```bash
# Create flamegraph for slow operation
cargo flamegraph --test performance test_bulk_insert
```

### 3. Analyze Memory Usage

Add `dhat` to specific tests and run:

```bash
cargo test --test performance test_bulk_insert -- --nocapture
```

### 4. Deep Dive with Instruments

```bash
# Use Instruments for detailed analysis
cargo instruments -t "Time Profiler" --test performance
```

### 5. Track Improvements

```bash
# Use criterion to track performance over time
cargo bench --bench performance
```

## Tips and Best Practices

1. **Always profile release builds**: Debug builds are not representative
   ```bash
   cargo flamegraph --release --bin gongdb
   ```

2. **Profile realistic workloads**: Use actual query patterns, not synthetic tests

3. **Compare before/after**: Keep profiles from before optimizations to compare

4. **Focus on hot paths**: Don't optimize code that's rarely executed

5. **Use multiple tools**: Different tools reveal different insights

6. **Profile incrementally**: Make small changes and re-profile

## Common Performance Issues

### High CPU Usage

- **Use flamegraph**: Identify which functions consume most CPU
- **Look for**: Tight loops, excessive allocations, inefficient algorithms

### Memory Issues

- **Use dhat**: Track allocation sites
- **Use Instruments Leaks**: Find memory leaks
- **Look for**: Unnecessary clones, large allocations, memory leaks

### Slow Queries

- **Profile specific queries**: Use flamegraph on query execution
- **Check indexes**: Verify indexes are being used
- **Analyze query plans**: Look for full table scans

## Example: Profiling a Slow Query

```bash
# 1. Create a test binary that runs the slow query
# (Add to src/bin/profile_query.rs)

# 2. Profile it
cargo flamegraph --bin profile_query

# 3. Open the flamegraph
open flamegraph.svg

# 4. Identify hotspots and optimize

# 5. Re-profile to verify improvement
cargo flamegraph --bin profile_query
```

## Troubleshooting

### Flamegraph Issues on macOS

If flamegraph doesn't work due to SIP:

```bash
# Option 1: Use cargo-instruments instead
cargo instruments -t "Time Profiler" --bin gongdb

# Option 2: Disable SIP (requires restart)
# System Preferences > Security & Privacy > General
# Then restart and try flamegraph again
```

### Permission Errors

```bash
# On Linux, you may need:
sudo sysctl kernel.perf_event_paranoid=-1

# On macOS with Instruments:
# May need to grant Terminal/iTerm permissions in System Preferences
```

### Missing Symbols

Ensure frame pointers are enabled:

```bash
RUSTFLAGS="-C force-frame-pointers=yes" cargo flamegraph --release
```

## Additional Resources

- [Flamegraph Documentation](https://github.com/flamegraph-rs/flamegraph)
- [cargo-instruments Documentation](https://github.com/cmyr/cargo-instruments)
- [dhat-rs Documentation](https://docs.rs/dhat/)
- [Criterion Documentation](https://github.com/bheisler/criterion.rs)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
