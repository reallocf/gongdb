#!/bin/bash
# Download all SQLite sqllogictest files

set -e

BASE_URL="https://www.sqlite.org/sqllogictest/raw"
BASE_DIR="tests/sqlite"

# Function to download files from a directory
download_dir() {
    local dir=$1
    local subdir=$2
    echo "Fetching file list from test/$dir..."
    local file_list=$(curl -s "https://www.sqlite.org/sqllogictest/dir?name=test/$dir" | grep -oE 'class="file[^"]*">[^<]+\.test' | sed 's/.*>//')
    
    if [ -z "$file_list" ]; then
        echo "No files found in test/$dir"
        return
    fi
    
    local count=0
    local total=$(echo "$file_list" | wc -l | tr -d ' ')
    echo "Downloading $total files from test/$dir..."
    
    while IFS= read -r file; do
        if [ -z "$file" ]; then continue; fi
        local target_dir="$BASE_DIR/$subdir"
        mkdir -p "$target_dir"
        local target_file="$target_dir/$file"
        
        # Skip if already downloaded
        if [ -f "$target_file" ] && [ -s "$target_file" ]; then
            ((count++))
            continue
        fi
        
        if curl -s "$BASE_URL?name=test/$dir/$file&ci=tip" -o "$target_file" 2>/dev/null && [ -s "$target_file" ]; then
            ((count++))
            if [ $((count % 10)) -eq 0 ]; then
                echo "  Progress: $count/$total files..."
            fi
        else
            echo "  Warning: Failed to download $file"
        fi
        sleep 0.15
    done <<< "$file_list"
    
    echo "Completed test/$dir: $count/$total files downloaded"
    echo ""
}

# Function to download files from nested index subdirectories
download_index_dir() {
    local index_dir=$1
    local subdir_base=$2
    echo "=== Downloading index/$index_dir ==="
    
    local total_count=0
    for size_dir in 1 10 100 1000 10000; do
        local dir_path="index/$index_dir/$size_dir"
        local file_list=$(curl -s "https://www.sqlite.org/sqllogictest/dir?name=test/$dir_path" 2>/dev/null | grep -oE 'class="file[^"]*">[^<]+\.test' | sed 's/.*>//')
        
        if [ -z "$file_list" ]; then
            continue
        fi
        
        local count=0
        local file_count=$(echo "$file_list" | wc -l | tr -d ' ')
        if [ "$file_count" -gt 0 ]; then
            echo "  Downloading $file_count files from $dir_path..."
        fi
        
        while IFS= read -r file; do
            if [ -z "$file" ]; then continue; fi
            local target_dir="$BASE_DIR/$subdir_base/$index_dir/$size_dir"
            mkdir -p "$target_dir"
            local target_file="$target_dir/$file"
            
            # Skip if already downloaded
            if [ -f "$target_file" ] && [ -s "$target_file" ]; then
                ((count++))
                continue
            fi
            
            if curl -s "$BASE_URL?name=test/$dir_path/$file&ci=tip" -o "$target_file" 2>/dev/null && [ -s "$target_file" ]; then
                ((count++))
            else
                echo "    Warning: Failed to download $file"
            fi
            sleep 0.15
        done <<< "$file_list"
        
        if [ "$file_count" -gt 0 ]; then
            echo "    Completed $size_dir: $count/$file_count files"
            total_count=$((total_count + count))
        fi
    done
    
    echo "  Total for index/$index_dir: $total_count files downloaded"
    echo ""
}

# Main test files (already downloaded, but check)
echo "=== Checking main test files ==="
download_dir "test" ""

# Evidence directory (already downloaded, but check)
echo "=== Checking evidence directory ==="
download_dir "evidence" "evidence"

# Random subdirectories
echo "=== Downloading random/aggregates ==="
download_dir "random/aggregates" "random/aggregates"

echo "=== Downloading random/expr ==="
download_dir "random/expr" "random/expr"

echo "=== Downloading random/groupby ==="
download_dir "random/groupby" "random/groupby"

echo "=== Downloading random/select ==="
download_dir "random/select" "random/select"

# Index subdirectories (nested structure)
download_index_dir "between" "index"
download_index_dir "commute" "index"
download_index_dir "delete" "index"
download_index_dir "in" "index"
download_index_dir "orderby" "index"
download_index_dir "orderby_nosort" "index"
download_index_dir "random" "index"
download_index_dir "view" "index"

echo ""
echo "=== Summary ==="
echo "Total test files: $(find tests/sqlite -name '*.test' -type f | wc -l | tr -d ' ')"
echo "Total size: $(du -sh tests/sqlite | cut -f1)"
