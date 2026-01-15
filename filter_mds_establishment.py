"""
Filter MDS files from quarterly folders for a specific establishment identifier.
Combines files from 2025_Q1 onwards, deduplicates by EPISODE ID.
"""
import os
import re
import pandas as pd
from pathlib import Path

# Configuration
SOURCE_FOLDER = r"C:\Users\aftab.jalal\Directions Health\Directions Health Intranet - Reporting\NADASurveyGenerator"
TARGET_ESTABLISHMENT = "12KK03112"
QUARTERS = ["2025_Q1", "2025_Q2", "2025_Q3", "2025_Q4", "2026_Q1"]
OUTPUT_FILE = os.path.join(SOURCE_FOLDER, "2026_Q1", f"MDS_{TARGET_ESTABLISHMENT}.csv")

# Patterns to exclude (version suffixes)
EXCLUDE_PATTERNS = [r'_v\d+', r'-prev', r'V\d+', r'AllPrograms\d+']

def is_main_file(filename: str) -> bool:
    """Check if file is a main file (no version suffix)."""
    basename = os.path.basename(filename)
    # Must contain AllPrograms
    if 'AllPrograms' not in basename:
        return False
    # Must end with AllPrograms.csv
    if not basename.endswith('AllPrograms.csv'):
        return False
    # Check for excluded patterns
    for pattern in EXCLUDE_PATTERNS:
        if re.search(pattern, basename):
            return False
    return True

def get_mds_files() -> list[str]:
    """Find all main MDS files from target quarters."""
    files = []
    for quarter in QUARTERS:
        quarter_path = os.path.join(SOURCE_FOLDER, quarter)
        if not os.path.exists(quarter_path):
            print(f"Warning: {quarter_path} does not exist")
            continue
        for file in os.listdir(quarter_path):
            if file.startswith("MDS_") and file.endswith(".csv"):
                full_path = os.path.join(quarter_path, file)
                if is_main_file(full_path):
                    files.append(full_path)
    return files

def extract_date_range(filename: str) -> tuple[str, str]:
    """Extract start and end date from MDS filename."""
    basename = os.path.basename(filename)
    match = re.search(r'MDS_(\d{8})-(\d{8})_', basename)
    if match:
        return match.group(1), match.group(2)
    return "", ""

def main():
    print(f"Filtering MDS files for establishment: {TARGET_ESTABLISHMENT}")
    print(f"Source folder: {SOURCE_FOLDER}")
    print(f"Quarters: {QUARTERS}")
    print()

    # Find all main MDS files
    mds_files = get_mds_files()
    print(f"Found {len(mds_files)} main MDS files:")
    for f in sorted(mds_files):
        print(f"  - {os.path.relpath(f, SOURCE_FOLDER)}")
    print()

    # Sort files by end date (descending) so latest files come first
    # When deduplicating, we keep first occurrence = latest data
    mds_files_sorted = sorted(mds_files, key=lambda x: extract_date_range(x)[1], reverse=True)

    # Read and filter each file
    all_dfs = []
    for file_path in mds_files_sorted:
        try:
            df = pd.read_csv(file_path, dtype=str, encoding='utf-8-sig')
            # Filter by establishment identifier
            filtered = df[df['ESTABLISHMENT IDENTIFIER'] == TARGET_ESTABLISHMENT]
            if len(filtered) > 0:
                print(f"  {os.path.relpath(file_path, SOURCE_FOLDER)}: {len(filtered)} rows")
                all_dfs.append(filtered)
            else:
                print(f"  {os.path.relpath(file_path, SOURCE_FOLDER)}: 0 rows (no matching establishment)")
        except Exception as e:
            print(f"  Error reading {file_path}: {e}")

    if not all_dfs:
        print(f"\nNo data found for establishment {TARGET_ESTABLISHMENT}")
        return

    # Combine all filtered dataframes
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal rows before deduplication: {len(combined)}")

    # Deduplicate by EPISODE ID (keep first = from latest file)
    deduped = combined.drop_duplicates(subset=['EPISODE ID'], keep='first')
    print(f"Total rows after deduplication: {len(deduped)}")

    # Save output
    deduped.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\nSaved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
