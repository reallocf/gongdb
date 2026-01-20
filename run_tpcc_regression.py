#!/usr/bin/env python3

"""
Script to run tpcc.rs test across a range of git commits
Usage: ./run_tpcc_regression.py <start_hash> <end_hash>
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def run_command(cmd, check=True, capture_output=False, **kwargs):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True,
            **kwargs
        )
        if capture_output:
            return result.stdout.strip()
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        if capture_output:
            return ""
        return False


def get_commits_between(start_hash, end_hash):
    """Get list of commits between two hashes (inclusive)."""
    if start_hash == end_hash:
        return [start_hash]
    
    # Try forward direction first
    commits = run_command(
        f'git log --reverse --format="%H" {start_hash}..{end_hash}',
        check=False,
        capture_output=True
    )
    
    if commits:
        # Include start hash
        commit_list = [start_hash] + [c for c in commits.split('\n') if c]
    else:
        # Try reverse direction
        reverse_commits = run_command(
            f'git log --reverse --format="%H" {end_hash}..{start_hash}',
            check=False,
            capture_output=True
        )
        if reverse_commits:
            commit_list = [c for c in reverse_commits.split('\n') if c]
            commit_list.reverse()
            commit_list.append(end_hash)
        else:
            print(f"Error: No commits found between {start_hash} and {end_hash}")
            print("  (They may not be on the same branch or may be unrelated)")
            sys.exit(1)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_commits = []
    for commit in commit_list:
        if commit and commit not in seen:
            seen.add(commit)
            unique_commits.append(commit)
    
    if not unique_commits:
        print(f"Error: No commits found between {start_hash} and {end_hash}")
        sys.exit(1)
    
    return unique_commits


def main():
    parser = argparse.ArgumentParser(
        description="Run tpcc.rs test across a range of git commits",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s abc123 def456
        """
    )
    parser.add_argument(
        "start_hash",
        help="The earlier commit hash (inclusive)"
    )
    parser.add_argument(
        "end_hash",
        help="The later commit hash (inclusive)"
    )
    
    args = parser.parse_args()
    
    start_hash = args.start_hash
    end_hash = args.end_hash
    
    # Validate that hashes exist
    if not run_command(f'git rev-parse --verify {start_hash}', check=False):
        print(f"Error: Invalid start hash: {start_hash}")
        sys.exit(1)
    
    if not run_command(f'git rev-parse --verify {end_hash}', check=False):
        print(f"Error: Invalid end hash: {end_hash}")
        sys.exit(1)
    
    # Save current branch/commit
    original_ref = run_command('git rev-parse --abbrev-ref HEAD', capture_output=True)
    original_commit = run_command('git rev-parse HEAD', capture_output=True)
    has_uncommitted = False
    
    # Check for uncommitted changes
    if not run_command('git diff-index --quiet HEAD --', check=False):
        print("Warning: Uncommitted changes detected. Stashing...")
        run_command('git stash push -m "Auto-stash before tpcc regression test"')
        has_uncommitted = True
    
    # Create output directory for results
    output_dir = Path(f"tpcc_regression_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file = output_dir / "regression.log"
    current_tpcc_rs = output_dir / "current_tpcc.rs"
    
    # Save current tpcc.rs to use consistently across all commits
    tpcc_rs_path = Path("tests/tpcc.rs")
    if tpcc_rs_path.exists():
        print("Saving current tests/tpcc.rs for consistent testing...")
        shutil.copy2(tpcc_rs_path, current_tpcc_rs)
        saved_tpcc_rs = True
    else:
        print("Warning: tests/tpcc.rs not found in current directory")
        saved_tpcc_rs = False
    
    # Cleanup tracking
    cleanup_done = False
    
    # Cleanup function
    def cleanup():
        nonlocal cleanup_done
        if cleanup_done:
            return
        cleanup_done = True
        
        print("\nCleaning up...")
        
        # Restore original state
        run_command(f'git checkout {original_ref}', check=False)
        if not run_command(f'git rev-parse --verify {original_ref}', check=False):
            run_command(f'git checkout {original_commit}', check=False)
        
        if has_uncommitted:
            print("Restoring uncommitted changes...")
            run_command('git stash pop', check=False)
        
        # Clean up saved tpcc.rs if it exists
        if current_tpcc_rs.exists():
            current_tpcc_rs.unlink()
        
        print(f"Cleanup complete. Results saved in: {output_dir}")
    
    # Set up signal handlers for cleanup
    def signal_handler(sig, frame):
        cleanup()
        sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Register cleanup on normal exit
    import atexit
    atexit.register(cleanup)
    
    # Get list of commits between start and end (inclusive)
    print(f"Getting commits between {start_hash} and {end_hash}...")
    commits = get_commits_between(start_hash, end_hash)
    
    commit_count = len(commits)
    print(f"Found {commit_count} commit(s) to test\n")
    
    # Process each commit
    for current, commit in enumerate(commits, 1):
        # Get commit info
        commit_date = run_command(
            f'git log -1 --format=%ci {commit}',
            capture_output=True
        )
        commit_msg = run_command(
            f'git log -1 --format=%s {commit}',
            capture_output=True
        ).replace(',', ';')
        
        print("=" * 42)
        print(f"[{current}/{commit_count}] Testing commit: {commit}")
        print(f"Date: {commit_date}")
        print(f"Message: {commit_msg}")
        print("=" * 42)
        
        # Checkout the commit
        if not run_command(f'git checkout {commit}', check=False):
            print(f"Error: Failed to checkout commit {commit}")
            continue
        
        # Restore current tpcc.rs for consistent testing
        if saved_tpcc_rs and current_tpcc_rs.exists():
            if tpcc_rs_path.exists():
                shutil.copy2(current_tpcc_rs, tpcc_rs_path)
            else:
                print("Warning: tests/tpcc.rs not found after checkout, skipping tpcc.rs restore")
        
        # Run the test and capture output to both stdout and file
        test_start = time.time()
        test_output_file = output_dir / f"commit_{commit[:8]}.log"
        
        # Run cargo test and tee output to both stdout and file
        test_status = "FAIL"
        try:
            with open(test_output_file, 'w') as f:
                # Run cargo test and write to both stdout and file
                process = subprocess.Popen(
                    ['cargo', 'test', '--test', 'tpcc', '--', '--nocapture'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )
                
                # Stream output to both stdout and file
                for line in process.stdout:
                    print(line, end='')
                    f.write(line)
                    f.flush()
                
                process.wait()
                if process.returncode == 0:
                    test_status = "PASS"
        except Exception as e:
            print(f"Error running test: {e}")
            test_status = "ERROR"
        
        test_end = time.time()
        elapsed = test_end - test_start
        
        # Log results
        print(f"Status: {test_status}")
        print(f"Elapsed: {elapsed:.2f}s")
        print()
        
        # Log to main log file
        with open(log_file, 'a') as f:
            f.write("=" * 42 + "\n")
            f.write(f"Commit: {commit}\n")
            f.write(f"Date: {commit_date}\n")
            f.write(f"Message: {commit_msg}\n")
            f.write(f"Status: {test_status}\n")
            f.write(f"Elapsed: {elapsed:.2f}s\n")
            f.write("=" * 42 + "\n\n")
    
    print()
    print("=" * 42)
    print("Regression test complete!")
    print(f"Results saved in: {output_dir}")
    print(f"  - {log_file} (detailed log)")
    print("  - Individual test outputs: commit_*.log")
    print("=" * 42)


if __name__ == "__main__":
    main()
