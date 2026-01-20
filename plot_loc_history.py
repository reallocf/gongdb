#!/usr/bin/env python3
"""Plot lines of code over time from git history CSV."""

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import sys

def parse_timestamp(ts_str):
    """Parse timestamp string from CSV (already shifted by 8 hours).
    
    Handles both formats:
    - "2026-01-20 00:02:33" (new format, no timezone)
    - "2026-01-20 00:02:33 -0500" (old format, with timezone)
    """
    ts_str = str(ts_str).strip()
    # Try new format first (no timezone)
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Fall back to old format with timezone
        try:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S %z")
            # If it has timezone, convert to naive datetime (already shifted in CSV)
            return dt.replace(tzinfo=None)
        except ValueError:
            # If both fail, strip timezone part and try again
            parts = ts_str.split()
            if len(parts) >= 2:
                return datetime.strptime(parts[0] + ' ' + parts[1], "%Y-%m-%d %H:%M:%S")
            raise

def create_plot(df, output_file):
    """Create and save a plot from the given dataframe.
    
    Args:
        df: DataFrame with 'datetime' and 'lines_of_code' columns
        output_file: Output filename for the plot
    """
    print(f"\nCreating plot: {output_file}")
    print(f"  Commits: {len(df)}")
    print(f"  Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    print(f"  Lines of code range: {df['lines_of_code'].min()} to {df['lines_of_code'].max()}")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Plot lines of code over time
    ax.plot(df['datetime'], df['lines_of_code'], 
            linewidth=2, marker='o', markersize=3, alpha=0.7)
    
    # Formatting
    ax.set_xlabel('Time', fontsize=20, fontweight='bold')
    ax.set_ylabel('Lines of Code', fontsize=20, fontweight='bold')
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis to show real timestamps
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right', fontsize=18)
    plt.yticks(fontsize=18)
    
    # Tight layout to prevent label cutoff
    plt.tight_layout()
    
    # Save figure
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved plot to {output_file}")
    
    # Show some statistics
    print("  Statistics:")
    print(f"    Initial LOC: {df['lines_of_code'].iloc[0]:,}")
    print(f"    Final LOC: {df['lines_of_code'].iloc[-1]:,}")
    print(f"    Total growth: {df['lines_of_code'].iloc[-1] - df['lines_of_code'].iloc[0]:,} lines")
    print(f"    Growth factor: {df['lines_of_code'].iloc[-1] / df['lines_of_code'].iloc[0]:.2f}x")
    
    # Calculate time span
    time_span = df['datetime'].iloc[-1] - df['datetime'].iloc[0]
    print(f"    Time span: {time_span.days} days, {time_span.seconds // 3600} hours")
    print(f"    Average LOC per day: {(df['lines_of_code'].iloc[-1] - df['lines_of_code'].iloc[0]) / max(time_span.days, 1):.1f}")

def main():
    csv_file = 'git_loc_history.csv'
    
    print(f"Reading {csv_file}...")
    df = pd.read_csv(csv_file)
    
    # Parse timestamps
    df['datetime'] = df['timestamp'].apply(parse_timestamp)
    
    # Sort by datetime (oldest first)
    df = df.sort_values('datetime')
    
    # Remove the first data point (oldest by timestamp)
    df = df.iloc[1:].reset_index(drop=True)
    
    print(f"\nLoaded {len(df)} commits (excluding first)")
    
    # Create full plot (all commits)
    create_plot(df, 'loc_history_plot.png')
    
    # Create filtered plot from separate CSV file
    early_csv_file = 'git_loc_history_early.csv'
    try:
        print(f"\nReading {early_csv_file}...")
        df_early = pd.read_csv(early_csv_file)
        df_early['datetime'] = df_early['timestamp'].apply(parse_timestamp)
        df_early = df_early.sort_values('datetime').reset_index(drop=True)
        print(f"Loaded {len(df_early)} commits from {early_csv_file}")
        create_plot(df_early, 'loc_history_plot_early.png')
    except FileNotFoundError:
        print(f"\nWarning: {early_csv_file} not found. Skipping early plot.")

if __name__ == '__main__':
    main()
