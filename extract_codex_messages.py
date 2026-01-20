#!/usr/bin/env python3

"""
Extract Codex Last Messages and TPCC Test Outputs from bead_runner.log

This script extracts all "--- Codex Last Message ---" blocks and TPCC test outputs
from the log file and associates them with their corresponding task/bead IDs. 
It processes the log file line-by-line to handle very large files efficiently.
"""

import re
import sys
from pathlib import Path
from typing import Optional, List, Tuple
from datetime import datetime


LOG_FILE = "bead_runner.log"
OUTPUT_FILE = "codex_messages.txt"


def parse_timestamp(line: str) -> Optional[str]:
    """Extract timestamp from log line. Returns timestamp string or None."""
    match = re.match(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', line)
    if match:
        return match.group(1)
    return None


def extract_message_content(line: str) -> str:
    """Extract message content from log line (remove timestamp prefix)."""
    match = re.match(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]\s*(.*)', line)
    if match:
        return match.group(1)
    return line.strip()


def extract_task_id_from_line(line: str) -> Optional[str]:
    """Extract task ID from 'Starting codex session for task: {task_id}' line."""
    match = re.search(r'Starting codex session for task: (\S+)', line)
    if match:
        return match.group(1)
    return None


def is_tpcc_test_command(line: str) -> bool:
    """Check if line contains a TPCC test command."""
    return 'cargo test --test tpcc -- --nocapture' in line or \
           'cargo test --test tpcc' in line


def is_command_execution(line: str) -> bool:
    """Check if line indicates a new command execution."""
    return '/bin/zsh -lc' in line and ('succeeded in' in line or 'failed in' in line)


def is_session_boundary(line: str) -> bool:
    """Check if line indicates a session boundary (new codex session or message block)."""
    return 'Starting codex session for task:' in line or \
           '--- Codex Last Message ---' in line or \
           '--- End Codex Last Message ---' in line


def extract_codex_messages(log_file: str) -> Tuple[List[Tuple[str, str, str, str]], List[Tuple[str, str, str, str]]]:
    """
    Extract all Codex Last Messages and TPCC test outputs from the log file.
    
    Returns tuple of:
    - List of Codex messages: (task_id, timestamp, message, raw_lines)
    - List of TPCC outputs: (task_id, timestamp, output, raw_lines)
    """
    messages = []
    tpcc_outputs = []
    current_task_id: Optional[str] = None
    capturing_message = False
    capturing_tpcc = False
    message_lines: List[str] = []
    tpcc_lines: List[str] = []
    message_start_timestamp: Optional[str] = None
    tpcc_start_timestamp: Optional[str] = None
    tpcc_command_line: Optional[str] = None
    
    log_path = Path(log_file)
    if not log_path.exists():
        print(f"Error: Log file not found: {log_file}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Processing {log_file}...", file=sys.stderr)
    print(f"File size: {log_path.stat().st_size / (1024*1024):.2f} MB", file=sys.stderr)
    
    line_count = 0
    with open(log_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            if line_count % 100000 == 0:
                print(f"  Processed {line_count:,} lines...", file=sys.stderr)
            
            # Track current task ID
            task_id = extract_task_id_from_line(line)
            if task_id:
                current_task_id = task_id
                # If we're capturing TPCC output and hit a new session, save what we have
                if capturing_tpcc and tpcc_lines:
                    tpcc_output = '\n'.join(tpcc_lines)
                    # Only keep TPCC entries where the benchmark test actually ran
                    if tpcc_output.strip() and "test test_tpcc_benchmark ... ok" in tpcc_output:
                        tpcc_outputs.append((
                            current_task_id or 'unknown',
                            tpcc_start_timestamp or 'unknown',
                            tpcc_output,
                            '\n'.join(tpcc_lines)
                        ))
                    capturing_tpcc = False
                    tpcc_lines = []
            
            # Check for TPCC test command execution
            # The pattern is: [timestamp] /bin/zsh -lc 'cargo test --test tpcc...' ... succeeded/failed in X.XXs:
            if is_command_execution(line) and is_tpcc_test_command(line):
                # If we were already capturing TPCC, save the previous one
                if capturing_tpcc and tpcc_lines:
                    tpcc_output = '\n'.join(tpcc_lines)
                    if tpcc_output.strip():
                        tpcc_outputs.append((
                            current_task_id or 'unknown',
                            tpcc_start_timestamp or 'unknown',
                            tpcc_output,
                            '\n'.join(tpcc_lines)
                        ))
                
                # Start capturing new TPCC output
                capturing_tpcc = True
                tpcc_lines = []
                tpcc_start_timestamp = parse_timestamp(line)
                tpcc_command_line = extract_message_content(line)
                # Include the command line in the output
                tpcc_lines.append(tpcc_command_line)
                continue
            
            # Check for start of Codex Last Message block
            if '--- Codex Last Message ---' in line:
                # If we're capturing TPCC, save it before starting message capture
                if capturing_tpcc and tpcc_lines:
                    tpcc_output = '\n'.join(tpcc_lines)
                    if tpcc_output.strip():
                        tpcc_outputs.append((
                            current_task_id or 'unknown',
                            tpcc_start_timestamp or 'unknown',
                            tpcc_output,
                            '\n'.join(tpcc_lines)
                        ))
                    capturing_tpcc = False
                    tpcc_lines = []
                
                if capturing_message:
                    # Previous message didn't have an end marker, save it anyway
                    if current_task_id and message_lines:
                        message_text = '\n'.join(message_lines)
                        messages.append((
                            current_task_id,
                            message_start_timestamp or 'unknown',
                            message_text,
                            '\n'.join(message_lines)
                        ))
                
                # Start capturing new message
                capturing_message = True
                message_lines = []
                message_start_timestamp = parse_timestamp(line)
                continue
            
            # Check for end of Codex Last Message block
            if '--- End Codex Last Message ---' in line:
                if capturing_message and current_task_id:
                    # Save the message
                    message_text = '\n'.join(message_lines)
                    if message_text.strip():  # Only save non-empty messages
                        messages.append((
                            current_task_id,
                            message_start_timestamp or 'unknown',
                            message_text,
                            '\n'.join(message_lines)
                        ))
                capturing_message = False
                message_lines = []
                continue
            
            # Check for boundaries that should end TPCC capture
            if capturing_tpcc:
                # If we see the benchmark test line, include it and then stop
                content = extract_message_content(line)
                if "test test_tpcc_benchmark ... ok" in content:
                    tpcc_lines.append(content)
                    tpcc_output = '\n'.join(tpcc_lines)
                    # Only keep TPCC entries where the benchmark test actually ran
                    if tpcc_output.strip() and "test test_tpcc_benchmark ... ok" in tpcc_output:
                        tpcc_outputs.append((
                            current_task_id or 'unknown',
                            tpcc_start_timestamp or 'unknown',
                            tpcc_output,
                            '\n'.join(tpcc_lines)
                        ))
                    capturing_tpcc = False
                    tpcc_lines = []
                    continue

                # Stop capturing if we hit a session boundary
                if is_session_boundary(line):
                    tpcc_output = '\n'.join(tpcc_lines)
                    # Only keep TPCC entries where the benchmark test actually ran
                    if tpcc_output.strip() and "test test_tpcc_benchmark ... ok" in tpcc_output:
                        tpcc_outputs.append((
                            current_task_id or 'unknown',
                            tpcc_start_timestamp or 'unknown',
                            tpcc_output,
                            '\n'.join(tpcc_lines)
                        ))
                    capturing_tpcc = False
                    tpcc_lines = []
                    # Don't continue - let the line be processed for session boundary
                elif is_command_execution(line):
                    # New command execution - stop capturing TPCC (unless it's another TPCC test)
                    if not is_tpcc_test_command(line):
                        tpcc_output = '\n'.join(tpcc_lines)
                        # Only keep TPCC entries where the benchmark test actually ran
                        if tpcc_output.strip() and "test test_tpcc_benchmark ... ok" in tpcc_output:
                            tpcc_outputs.append((
                                current_task_id or 'unknown',
                                tpcc_start_timestamp or 'unknown',
                                tpcc_output,
                                '\n'.join(tpcc_lines)
                            ))
                        capturing_tpcc = False
                        tpcc_lines = []
                        # Continue to process this line as a new command
                    # If it's another TPCC test, the logic above will handle it
                else:
                    # Continue capturing TPCC output
                    tpcc_lines.append(content)
                    continue
            
            # If we're capturing message, add this line to the message
            if capturing_message:
                # Remove timestamp prefix from the line
                content = extract_message_content(line)
                message_lines.append(content)
    
    # Save any remaining captures
    if capturing_tpcc and tpcc_lines:
        tpcc_output = '\n'.join(tpcc_lines)
        # Only keep TPCC entries where the benchmark test actually ran
        if tpcc_output.strip() and "test test_tpcc_benchmark ... ok" in tpcc_output:
            tpcc_outputs.append((
                current_task_id or 'unknown',
                tpcc_start_timestamp or 'unknown',
                tpcc_output,
                '\n'.join(tpcc_lines)
            ))
    
    if capturing_message and message_lines and current_task_id:
        message_text = '\n'.join(message_lines)
        if message_text.strip():
            messages.append((
                current_task_id,
                message_start_timestamp or 'unknown',
                message_text,
                '\n'.join(message_lines)
            ))
    
    print(f"  Total lines processed: {line_count:,}", file=sys.stderr)
    print(f"  Found {len(messages)} Codex Last Messages", file=sys.stderr)
    print(f"  Found {len(tpcc_outputs)} TPCC test outputs", file=sys.stderr)
    
    return messages, tpcc_outputs


def format_output(messages: List[Tuple[str, str, str, str]], 
                  tpcc_outputs: List[Tuple[str, str, str, str]]) -> str:
    """Format messages and TPCC outputs for output."""
    output_lines = []
    
    # Group outputs by task ID and timestamp to show them together
    # Create a combined list with type indicators
    all_items = []
    for msg in messages:
        all_items.append(('message', msg))
    for tpcc in tpcc_outputs:
        all_items.append(('tpcc', tpcc))
    
    # Sort by timestamp (second element of tuple)
    all_items.sort(key=lambda x: x[1][1])  # Sort by timestamp
    
    # Format each item
    message_count = 0
    tpcc_count = 0
    
    for item_type, (task_id, timestamp, content, _) in all_items:
        if item_type == 'message':
            message_count += 1
            output_lines.append("=" * 80)
            output_lines.append(f"Codex Message {message_count} of {len(messages)}")
            output_lines.append("=" * 80)
            output_lines.append(f"Task ID: {task_id}")
            output_lines.append(f"Timestamp: {timestamp}")
            output_lines.append("")
            output_lines.append("Message:")
            output_lines.append("-" * 80)
            output_lines.append(content)
            output_lines.append("-" * 80)
        else:  # tpcc
            tpcc_count += 1
            output_lines.append("=" * 80)
            output_lines.append(f"TPCC Test Output {tpcc_count} of {len(tpcc_outputs)}")
            output_lines.append("=" * 80)
            output_lines.append(f"Task ID: {task_id}")
            output_lines.append(f"Timestamp: {timestamp}")
            output_lines.append("")
            output_lines.append("TPCC Test Output:")
            output_lines.append("-" * 80)
            output_lines.append(content)
            output_lines.append("-" * 80)
        
        output_lines.append("")
        output_lines.append("")
    
    return '\n'.join(output_lines)


def main():
    """Main function"""
    if len(sys.argv) > 1:
        log_file = sys.argv[1]
    else:
        log_file = LOG_FILE
    
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    else:
        output_file = OUTPUT_FILE
    
    # Extract messages and TPCC outputs
    messages, tpcc_outputs = extract_codex_messages(log_file)
    
    if not messages and not tpcc_outputs:
        print("No Codex Last Messages or TPCC test outputs found in the log file.", file=sys.stderr)
        return
    
    # Format and write output
    output = format_output(messages, tpcc_outputs)
    
    output_path = Path(output_file)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(output)
    
    print(f"\nExtracted {len(messages)} messages and {len(tpcc_outputs)} TPCC outputs to: {output_file}", file=sys.stderr)
    
    # Also print summary to stdout
    print(f"\nSummary:")
    print(f"  Total Codex messages: {len(messages)}")
    print(f"  Total TPCC test outputs: {len(tpcc_outputs)}")
    if messages:
        print(f"  Unique tasks (messages): {len(set(task_id for task_id, _, _, _ in messages))}")
    if tpcc_outputs:
        print(f"  Unique tasks (TPCC): {len(set(task_id for task_id, _, _, _ in tpcc_outputs))}")
    print(f"  Output file: {output_file}")


if __name__ == '__main__':
    main()
