#!/usr/bin/env python3
"""Analyze git history and count lines of code per commit."""

import subprocess
import csv
import sys
from datetime import datetime, timedelta

def get_commits(branch):
    """Get all commits for a branch."""
    result = subprocess.run(
        ['git', 'log', '--format=%H|%ai|%s', branch],
        capture_output=True,
        text=True,
        check=True
    )
    commits = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split('|', 2)
        if len(parts) == 3:
            # Parse timestamp and shift back by 8 hours
            # Git %ai format: "2024-01-20 14:30:00 -0800" or "2024-01-20 14:30:00"
            ts_str = parts[1].strip()
            try:
                # Try parsing with timezone (format: "YYYY-MM-DD HH:MM:SS +/-HHMM")
                if len(ts_str) > 19 and ts_str[19] == ' ':
                    # Has timezone info
                    dt_part = ts_str[:19]
                    tz_part = ts_str[19:].strip()
                    ts = datetime.strptime(dt_part, '%Y-%m-%d %H:%M:%S')
                    # Parse timezone offset (format: "+0800" or "-0800")
                    if tz_part:
                        tz_sign = 1 if tz_part[0] == '+' else -1
                        tz_hours = int(tz_part[1:3])
                        tz_mins = int(tz_part[3:5])
                        tz_offset = timedelta(hours=tz_hours * tz_sign, minutes=tz_mins * tz_sign)
                        ts = ts - tz_offset  # Convert to UTC
                else:
                    # No timezone info, assume UTC
                    ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                # Fallback: try fromisoformat
                ts = datetime.fromisoformat(ts_str.replace(' ', 'T', 1))
            ts_shifted = ts - timedelta(hours=8)
            commits.append({
                'hash': parts[0],
                'timestamp': ts_shifted.strftime('%Y-%m-%d %H:%M:%S'),
                'message': parts[2]
            })
    return commits

def count_lines_of_code(commit_hash):
    """Count lines of code in Rust source files for a given commit."""
    # Get list of .rs files in src/ directory at this commit
    try:
        result = subprocess.run(
            ['git', 'ls-tree', '-r', '--name-only', commit_hash, 'src/'],
            capture_output=True,
            text=True,
            check=True
        )
        
        total_lines = 0
        for file_path in result.stdout.strip().split('\n'):
            if not file_path or not file_path.endswith('.rs'):
                continue
            try:
                # Get file contents from git
                file_result = subprocess.run(
                    ['git', 'show', f'{commit_hash}:{file_path}'],
                    capture_output=True,
                    text=True,
                    check=True
                )
                lines = file_result.stdout.split('\n')
                total_lines += len(lines)
            except subprocess.CalledProcessError:
                # File might not exist at this commit or is binary
                continue
            except Exception as e:
                print(f"Warning: Could not read {file_path}: {e}", file=sys.stderr)
        
        return total_lines
    except subprocess.CalledProcessError:
        # No src directory or no .rs files at this commit
        return 0

def main():
    branch = 'probable-end-point'
    
    print(f"Getting commit history for branch: {branch}")
    commits = get_commits(branch)
    print(f"Found {len(commits)} commits")
    
    csv_data = []
    
    for i, commit in enumerate(commits, 1):
        print(f"Processing commit {i}/{len(commits)}: {commit['hash'][:8]} - {commit['message'][:50]}...")
        loc = count_lines_of_code(commit['hash'])
        csv_data.append({
            'commit_hash': commit['hash'],
            'timestamp': commit['timestamp'],
            'message': commit['message'],
            'lines_of_code': loc
        })
    
    # Write CSV
    output_file = 'git_loc_history.csv'
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['commit_hash', 'timestamp', 'message', 'lines_of_code'])
        writer.writeheader()
        writer.writerows(csv_data)
    
    print(f"\nWrote {len(csv_data)} commits to {output_file}")

if __name__ == '__main__':
    main()
