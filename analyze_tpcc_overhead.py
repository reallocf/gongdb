#!/usr/bin/env python3
"""
Analyze TPC-C overhead data and calculate averages over time.

This script reads tpcc_overhead.csv and calculates averages of the overhead
columns, both overall and grouped by time periods.
"""

import csv
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from statistics import mean
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def parse_timestamp(ts_str):
    """Parse timestamp string to datetime object and shift back by 8 hours."""
    dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
    return dt - timedelta(hours=8)


def read_overhead_data(csv_path):
    """Read overhead data from CSV file."""
    data = []
    overhead_columns = [
        'setup_overhead', 'new_order_overhead', 'payment_overhead',
        'order_status_overhead', 'stock_level_overhead', 'delivery_overhead'
    ]
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip rows without timestamp
            if not row.get('timestamp'):
                continue
            
            try:
                row['timestamp'] = parse_timestamp(row['timestamp'])
            except (ValueError, TypeError):
                continue
            
            # Convert overhead values to floats, skip empty values
            all_present = True
            for key in overhead_columns:
                if row.get(key) and row[key].strip():
                    try:
                        row[key] = float(row[key])
                    except (ValueError, TypeError):
                        all_present = False
                        break
                else:
                    all_present = False
                    break
            
            # Only include rows that have ALL overhead columns (complete rows)
            if all_present:
                data.append(row)
    
    return data


def calculate_overall_averages(data):
    """Calculate overall averages for all overhead columns."""
    overhead_columns = [
        'setup_overhead', 'new_order_overhead', 'payment_overhead',
        'order_status_overhead', 'stock_level_overhead', 'delivery_overhead'
    ]
    
    averages = {}
    for col in overhead_columns:
        values = [row[col] for row in data]
        averages[col] = mean(values)
    
    return averages


def calculate_time_grouped_averages(data, group_by='hour'):
    """Calculate averages grouped by time period."""
    overhead_columns = [
        'setup_overhead', 'new_order_overhead', 'payment_overhead',
        'order_status_overhead', 'stock_level_overhead', 'delivery_overhead'
    ]
    
    grouped_data = defaultdict(lambda: {col: [] for col in overhead_columns})
    
    for row in data:
        ts = row['timestamp']
        
        if group_by == 'hour':
            key = ts.strftime('%Y-%m-%d %H:00')
        elif group_by == 'day':
            key = ts.strftime('%Y-%m-%d')
        else:
            raise ValueError(f"Unknown group_by value: {group_by}")
        
        for col in overhead_columns:
            grouped_data[key][col].append(row[col])
    
    # Calculate averages for each time period
    results = []
    for time_key in sorted(grouped_data.keys()):
        period_avg = {'time_period': time_key}
        for col in overhead_columns:
            period_avg[col] = mean(grouped_data[time_key][col])
        results.append(period_avg)
    
    return results


def print_averages(title, averages):
    """Print averages in a formatted table."""
    print(f"\n{title}")
    print("=" * 80)
    print(f"{'Metric':<30} {'Average Overhead':>20}")
    print("-" * 80)
    
    for col, avg in averages.items():
        metric_name = col.replace('_', ' ').title()
        print(f"{metric_name:<30} {avg:>20.2f}x")
    
    print("=" * 80)


def print_time_grouped_averages(grouped_averages, group_by='hour'):
    """Print time-grouped averages in a formatted table."""
    period_name = group_by.title()
    print(f"\nAverages by {period_name}")
    print("=" * 120)
    
    # Header
    header = f"{period_name:<20}"
    columns = [
        'setup_overhead', 'new_order_overhead', 'payment_overhead',
        'order_status_overhead', 'stock_level_overhead', 'delivery_overhead'
    ]
    for col in columns:
        header += f" {col.replace('_', ' ').title():>15}"
    print(header)
    print("-" * 120)
    
    # Data rows
    for period_data in grouped_averages:
        row = f"{period_data['time_period']:<20}"
        for col in columns:
            row += f" {period_data[col]:>15.2f}x"
        print(row)
    
    print("=" * 120)


def create_graph(data, output_path):
    """Create a single graph showing average of all overhead columns over time."""
    overhead_columns = [
        'setup_overhead', 'new_order_overhead', 'payment_overhead',
        'order_status_overhead', 'stock_level_overhead', 'delivery_overhead'
    ]
    
    # Sort data by timestamp
    data_sorted = sorted(data, key=lambda x: x['timestamp'])
    
    timestamps = [row['timestamp'] for row in data_sorted]
    
    # Calculate average overhead for each timestamp
    avg_overhead = []
    for row in data_sorted:
        values = [row[col] for col in overhead_columns]
        avg_overhead.append(mean(values))
    
    # Create single graph
    fig, ax = plt.subplots(figsize=(14, 8))
    
    ax.plot(timestamps, avg_overhead, marker='o', markersize=5, linewidth=2, 
            color='#1f77b4', alpha=0.8)
    
    ax.set_xlabel('Time', fontsize=20)
    ax.set_ylabel('Overhead Relative to SQLite', fontsize=20)
    ax.grid(True, alpha=0.3)
    
    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=18)
    plt.setp(ax.yaxis.get_majorticklabels(), fontsize=18)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nGraph saved to {output_path}")


def create_filtered_graph(data, output_path, filter_timestamp_str):
    """Create a graph showing average overhead after a specific timestamp."""
    overhead_columns = [
        'setup_overhead', 'new_order_overhead', 'payment_overhead',
        'order_status_overhead', 'stock_level_overhead', 'delivery_overhead'
    ]
    
    # Parse filter timestamp
    filter_timestamp = parse_timestamp(filter_timestamp_str)
    
    # Filter data after the filter timestamp
    filtered_data = [row for row in data if row['timestamp'] > filter_timestamp]
    
    if not filtered_data:
        print(f"No data found after {filter_timestamp_str}")
        return
    
    # Sort data by timestamp
    data_sorted = sorted(filtered_data, key=lambda x: x['timestamp'])
    
    timestamps = [row['timestamp'] for row in data_sorted]
    
    # Calculate average overhead for each timestamp
    avg_overhead = []
    for row in data_sorted:
        values = [row[col] for col in overhead_columns]
        avg_overhead.append(mean(values))
    
    # Create single graph
    fig, ax = plt.subplots(figsize=(14, 8))
    
    ax.plot(timestamps, avg_overhead, marker='o', markersize=5, linewidth=2, 
            color='#1f77b4', alpha=0.8)
    
    # No x-axis title or tick labels, keep y-axis tick labels (but no title)
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.grid(True, alpha=0.3)
    
    # Hide x-axis tick labels
    ax.set_xticklabels([])
    ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
    
    # Set y-axis tick label font size
    plt.setp(ax.yaxis.get_majorticklabels(), fontsize=18)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Filtered graph saved to {output_path}")


def main():
    script_dir = Path(__file__).parent
    csv_file = script_dir / 'tpcc_overhead.csv'
    
    if not csv_file.exists():
        print(f"Error: {csv_file} not found")
        return 1
    
    print(f"Reading {csv_file}...")
    data = read_overhead_data(csv_file)
    
    if not data:
        print("No data found in CSV file.")
        return 1
    
    print(f"Loaded {len(data)} test runs")
    
    # Calculate overall averages
    overall_avg = calculate_overall_averages(data)
    print_averages("Overall Averages", overall_avg)
    
    # Calculate hourly averages
    hourly_avg = calculate_time_grouped_averages(data, group_by='hour')
    if len(hourly_avg) > 1:
        print_time_grouped_averages(hourly_avg, group_by='hour')
    
    # Calculate daily averages
    daily_avg = calculate_time_grouped_averages(data, group_by='day')
    if len(daily_avg) > 1:
        print_time_grouped_averages(daily_avg, group_by='day')
    
    # Create graph
    graph_file = script_dir / 'tpcc_overhead_graph.png'
    create_graph(data, graph_file)
    
    # Create filtered graph (after 2026-01-20 06:29:00)
    filtered_graph_file = script_dir / 'tpcc_overhead_graph_filtered.png'
    create_filtered_graph(data, filtered_graph_file, '2026-01-20 06:29:00')
    
    return 0


if __name__ == '__main__':
    exit(main())
