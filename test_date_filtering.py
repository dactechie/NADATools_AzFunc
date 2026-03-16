"""
Test to confirm the date filtering issue
"""
import pandas as pd
from datetime import datetime

print("Testing date filtering issue")
print("="*60)

# Read the matched CSV file
csv_file = r"C:\Users\aftab.jalal\dev\NADATools_AzFunc\data\processed\forstxt_20250201-20250331_matched.csv"
df_matched = pd.read_csv(csv_file)

print(f"Total input rows: {len(df_matched)}")

# Check episode 160550
episode_160550 = df_matched[df_matched['PMSEpisodeID'] == 160550]
if not episode_160550.empty:
    assessment_date = episode_160550.iloc[0]['AssessmentDate']
    print(f"\nEpisode 160550:")
    print(f"  Client ID: {episode_160550.iloc[0]['PMSPersonID']}")
    print(f"  Assessment Date: {assessment_date}")

# Now test the date filtering
reporting_start_str = "20250201"
reporting_end_str = "20250331"

from assessment_episode_matcher.utils.fromstr import get_date_from_str

reporting_start = get_date_from_str(reporting_start_str, "%Y%m%d")
reporting_end = get_date_from_str(reporting_end_str, "%Y%m%d")

print(f"\nDate range for filtering:")
print(f"  Start: {reporting_start}")
print(f"  End: {reporting_end}")

# Apply the same filtering as in nada_helper.py
asmtdt_field = 'AssessmentDate'
reporting_start_ts = pd.Timestamp(reporting_start)
reporting_end_ts = pd.Timestamp(reporting_end)

print(f"\nDate range as timestamps:")
print(f"  Start: {reporting_start_ts}")
print(f"  End: {reporting_end_ts}")

# Convert assessment dates to datetime
df_matched['AssessmentDate_dt'] = pd.to_datetime(df_matched[asmtdt_field])

# Check episode 160550's date
if not episode_160550.empty:
    ep_date = pd.to_datetime(episode_160550.iloc[0]['AssessmentDate'])
    print(f"\nEpisode 160550 assessment date as timestamp: {ep_date}")
    print(f"  Is >= start date? {ep_date >= reporting_start_ts}")
    print(f"  Is <= end date? {ep_date <= reporting_end_ts}")
    print(f"  Will be included? {(ep_date >= reporting_start_ts) and (ep_date <= reporting_end_ts)}")

# Apply the filter
filtered_assessments = df_matched[
    (df_matched['AssessmentDate_dt'] >= reporting_start_ts) &
    (df_matched['AssessmentDate_dt'] <= reporting_end_ts)
]

print(f"\nAfter date filtering:")
print(f"  Output rows: {len(filtered_assessments)}")
print(f"  Rows dropped: {len(df_matched) - len(filtered_assessments)}")

# Check if episode 160550 is in filtered data
episode_160550_filtered = filtered_assessments[filtered_assessments['PMSEpisodeID'] == 160550]
if not episode_160550_filtered.empty:
    print(f"  [OK] Episode 160550 is present after date filtering")
else:
    print(f"  [MISSING] Episode 160550 was dropped by date filtering!")
    print(f"  REASON: Assessment date 2025-01-31 is BEFORE the start date 2025-02-01")

# Show distribution of assessment dates
print(f"\nAssessment date range in input file:")
print(f"  Min date: {df_matched['AssessmentDate_dt'].min()}")
print(f"  Max date: {df_matched['AssessmentDate_dt'].max()}")

# Count rows outside the requested range
before_range = df_matched[df_matched['AssessmentDate_dt'] < reporting_start_ts]
after_range = df_matched[df_matched['AssessmentDate_dt'] > reporting_end_ts]

print(f"\nRows with dates before {reporting_start}: {len(before_range)}")
print(f"Rows with dates after {reporting_end}: {len(after_range)}")

if len(before_range) > 0:
    print(f"\nSample of rows with dates before range:")
    for idx, row in before_range.head(10).iterrows():
        print(f"  Episode {row['PMSEpisodeID']}: {row['AssessmentDate']}")
