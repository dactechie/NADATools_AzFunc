"""
Simple test to identify why rows are skipped in surveytxt generation
Uses local CSV file instead of Azure blob storage
"""
import logging
import pandas as pd
from assessment_episode_matcher.data_prep import get_surveydata_expanded
from assessment_episode_matcher.mytypes import Purpose

logging.basicConfig(level=logging.INFO)

print("Testing row skipping issue")
print("="*60)

# Read the matched CSV file directly
csv_file = r"C:\Users\aftab.jalal\dev\NADATools_AzFunc\data\processed\forstxt_20250201-20250331_matched.csv"

print(f"Reading: {csv_file}")
df_matched = pd.read_csv(csv_file)

print(f"\nInput data:")
print(f"  Total rows: {len(df_matched)}")

# Check if episode 160550 is in the input
episode_160550 = df_matched[df_matched['PMSEpisodeID'] == 160550]
if not episode_160550.empty:
    print(f"  [OK] Episode 160550 found in input")
    print(f"    - Client ID: {episode_160550.iloc[0]['PMSPersonID']}")
    print(f"    - SLK: {episode_160550.iloc[0]['SLK']}")
else:
    print(f"  [MISSING] Episode 160550 NOT found in input")

print("\n" + "="*60)
print("Testing get_surveydata_expanded (where filtering happens)...")
print("="*60)

# This is where rows might get dropped
df_expanded = get_surveydata_expanded(df_matched.copy(), Purpose.NADA)

print(f"\nAfter get_surveydata_expanded:")
print(f"  Output rows: {len(df_expanded)}")
print(f"  Rows dropped: {len(df_matched) - len(df_expanded)}")

# Check if our specific episode is in the expanded data
episode_160550_expanded = df_expanded[df_expanded['PMSEpisodeID'] == 160550]
if not episode_160550_expanded.empty:
    print(f"  [OK] Episode 160550 is present in expanded data")
else:
    print(f"  [MISSING] Episode 160550 is MISSING from expanded data")

    if not episode_160550.empty:
        print(f"\n  Investigating why it was dropped...")
        row = episode_160550.iloc[0]
        print(f"  - Row index: {episode_160550.index[0]}")

        survey_data = row.get('SurveyData', '')
        print(f"  - SurveyData length: {len(str(survey_data))}")
        print(f"  - SurveyData first 300 chars: {str(survey_data)[:300]}...")

        # Try to parse it
        from assessment_episode_matcher.utils.fromstr import clean_and_parse_json
        try:
            parsed = clean_and_parse_json(survey_data)
            print(f"  - Parsed successfully: {type(parsed)}")
            print(f"  - Is dict: {isinstance(parsed, dict)}")

            if not isinstance(parsed, dict):
                print(f"  - REASON: SurveyData did not parse to a dict!")
                print(f"  - Actual type: {type(parsed)}")
                print(f"  - Value: {parsed}")
        except Exception as e:
            print(f"  - REASON: Failed to parse JSON!")
            print(f"  - Error: {e}")

# Show some examples of rows that were dropped
print(f"\n" + "="*60)
print("Analyzing ALL dropped rows...")
print("="*60)

# Create a mask of valid survey data
df_surveydata = df_matched['SurveyData'].apply(lambda x: x if pd.notna(x) else '')

from assessment_episode_matcher.utils.fromstr import clean_and_parse_json
parsed_data = df_surveydata.apply(clean_and_parse_json)
valid_mask = parsed_data.apply(lambda x: isinstance(x, dict))

dropped_count = (~valid_mask).sum()
print(f"\nTotal rows dropped: {dropped_count}")

if dropped_count > 0:
    print(f"\nFirst 5 dropped rows:")
    dropped_rows = df_matched[~valid_mask].head(5)
    for idx, row in dropped_rows.iterrows():
        print(f"\n  Row {idx}:")
        print(f"    Episode: {row.get('PMSEpisodeID')}")
        print(f"    Client: {row.get('PMSPersonID')}")
        print(f"    SLK: {row.get('SLK')}")
        survey = str(row.get('SurveyData', ''))
        print(f"    SurveyData preview: {survey[:100]}...")

        # Try parsing
        try:
            parsed = clean_and_parse_json(survey)
            print(f"    Parsed type: {type(parsed)}")
        except Exception as e:
            print(f"    Parse error: {e}")
