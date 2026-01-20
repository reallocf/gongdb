#!/usr/bin/env python3
"""
Extract TPC-C overhead multipliers from codex_messages.txt and output to CSV.

This script parses codex_messages.txt to find TPC-C benchmark results and extracts
the overhead multipliers compared to rusqlite for each transaction type.
"""

import re
import csv
from pathlib import Path
from datetime import datetime, timedelta


def extract_multiplier(line):
    """Extract the rusqlite multiplier from a line like 'vs rusqlite=123.45x'."""
    match = re.search(r'vs rusqlite=([\d.]+)x', line)
    if match:
        return float(match.group(1))
    return None


def extract_timestamp(line):
    """Extract timestamp from a line like 'Timestamp: 2026-01-19 16:13:10' and shift by +8 hours."""
    match = re.search(r'^Timestamp:\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
    if match:
        timestamp_str = match.group(1)
        # Parse timestamp and shift by +8 hours
        try:
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            dt_shifted = dt + timedelta(hours=8)
            return dt_shifted.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return timestamp_str  # Return original if parsing fails
    return None


def parse_codex_messages(file_path):
    """Parse codex_messages.txt and extract TPC-C overhead data."""
    results = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        current_run = {}
        in_tpcc_section = False
        section_timestamp = None
        
        for line in f:
            line_stripped = line.strip()
            
            # Check if we're entering a TPCC Test Output section
            if line_stripped.startswith('TPCC Test Output') and 'of 134' in line_stripped:
                # If we were in a previous section, save it (even if empty)
                if in_tpcc_section:
                    # Save if we have at least a timestamp (even if no transaction data)
                    if 'timestamp' in current_run:
                        results.append(current_run.copy())
                    elif section_timestamp:
                        # Save with just timestamp if we have one
                        results.append({'timestamp': section_timestamp})
                
                # Start a new section
                in_tpcc_section = True
                current_run = {}
                section_timestamp = None
            
            # If we're in a TPCC section, look for timestamp
            elif in_tpcc_section and line_stripped.startswith('Timestamp:'):
                timestamp = extract_timestamp(line_stripped)
                if timestamp:
                    section_timestamp = timestamp
                    current_run['timestamp'] = timestamp
            
            # Check for each transaction type (only if we're in a TPCC section)
            elif in_tpcc_section:
                if line_stripped.startswith('TPC-C Setup:'):
                    # If we haven't captured timestamp yet, use the section timestamp
                    if 'timestamp' not in current_run and section_timestamp:
                        current_run['timestamp'] = section_timestamp
                    
                    multiplier = extract_multiplier(line_stripped)
                    if multiplier is not None:
                        current_run['setup_overhead'] = multiplier
                
                elif line_stripped.startswith('New Order Transaction:'):
                    multiplier = extract_multiplier(line_stripped)
                    if multiplier is not None:
                        current_run['new_order_overhead'] = multiplier
                
                elif line_stripped.startswith('Payment Transaction:'):
                    multiplier = extract_multiplier(line_stripped)
                    if multiplier is not None:
                        current_run['payment_overhead'] = multiplier
                
                elif line_stripped.startswith('Order Status Transaction:'):
                    multiplier = extract_multiplier(line_stripped)
                    if multiplier is not None:
                        current_run['order_status_overhead'] = multiplier
                
                elif line_stripped.startswith('Stock Level Transaction:'):
                    multiplier = extract_multiplier(line_stripped)
                    if multiplier is not None:
                        current_run['stock_level_overhead'] = multiplier
                
                elif line_stripped.startswith('Delivery Transaction:'):
                    multiplier = extract_multiplier(line_stripped)
                    if multiplier is not None:
                        current_run['delivery_overhead'] = multiplier
                    
                    # When we find Delivery Transaction, we've completed a full test run
                    # Save if we have timestamp and at least one transaction type
                    if 'timestamp' in current_run and any(key in current_run for key in ['setup_overhead', 'new_order_overhead', 'payment_overhead', 'order_status_overhead', 'stock_level_overhead', 'delivery_overhead']):
                        results.append(current_run.copy())
                    
                    # Reset for next run (but stay in TPCC section mode)
                    current_run = {}
                    section_timestamp = None
                
                # Check if we're leaving the TPCC section (new major section starts)
                elif line_stripped.startswith('Codex Message') or (line_stripped.startswith('Message:') and not line_stripped.startswith('TPCC Test Output')):
                    # Save if we have at least a timestamp (even if no transaction data)
                    if 'timestamp' in current_run:
                        results.append(current_run.copy())
                    elif section_timestamp:
                        # Save with just timestamp if we have one
                        results.append({'timestamp': section_timestamp})
                    
                    in_tpcc_section = False
                    current_run = {}
                    section_timestamp = None
        
        # Handle the last section if file ends while in a TPCC section
        if in_tpcc_section:
            # Save if we have at least a timestamp (even if no transaction data)
            if 'timestamp' in current_run:
                results.append(current_run.copy())
            elif section_timestamp:
                # Save with just timestamp if we have one
                results.append({'timestamp': section_timestamp})
    
    return results


def write_csv(results, output_path):
    """Write results to CSV file."""
    if not results:
        print("No results found to write.")
        return
    
    fieldnames = [
        'timestamp',
        'setup_overhead',
        'new_order_overhead',
        'payment_overhead',
        'order_status_overhead',
        'stock_level_overhead',
        'delivery_overhead'
    ]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for result in results:
            # Ensure all fields are present (use empty string for missing values)
            row = {field: result.get(field, '') for field in fieldnames}
            writer.writerow(row)
    
    complete = sum(1 for r in results if all(r.get(f) for f in fieldnames[1:]))  # All overhead fields present
    partial = sum(1 for r in results if any(r.get(f) for f in fieldnames[1:]) and not all(r.get(f) for f in fieldnames[1:]))  # Some but not all
    empty = len(results) - complete - partial  # No transaction data
    print(f"Wrote {len(results)} test runs to {output_path} ({complete} complete, {partial} partial, {empty} empty)")


def main():
    script_dir = Path(__file__).parent
    input_file = script_dir / 'codex_messages.txt'
    output_file = script_dir / 'tpcc_overhead.csv'
    
    if not input_file.exists():
        print(f"Error: {input_file} not found")
        return 1
    
    print(f"Parsing {input_file}...")
    results = parse_codex_messages(input_file)
    
    if not results:
        print("No TPC-C overhead data found in the file.")
        return 1
    
    print(f"Found {len(results)} complete test runs")
    write_csv(results, output_file)
    
    return 0


if __name__ == '__main__':
    exit(main())
