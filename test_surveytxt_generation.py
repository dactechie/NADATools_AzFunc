"""
Test script to identify why rows are skipped in surveytxt generation
"""
import logging
logging.basicConfig(level=logging.INFO)

# Test the surveytxt generation process
start_dt = "20250201"
end_dt = "20250331"

print(f"Testing surveytxt generation for {start_dt} to {end_dt}")
print("="*60)

# Import the helper function
import nada_helper

result = nada_helper.run(start_yyyymmd=start_dt, end_yyyymmd=end_dt)

print("\nResult:")
print(result)
print("="*60)

# Now let's check the intermediate steps
container_name = "atom-matching"
df_matched, fname = nada_helper.get_matched_assessments(container_name, start_dt, end_dt)

print(f"\nInput file: {fname}")
print(f"Input rows: {len(df_matched)}")

# Now let's check what happens during prep_nada_fields
import os
from assessment_episode_matcher.configs import load_blob_config

config = load_blob_config(container_name)
if not config:
    print("WARNING: Could not load config")
else:
    from assessment_episode_matcher.data_prep import prep_nada_fields, get_surveydata_expanded
    from assessment_episode_matcher.mytypes import Purpose

    print("\nTesting get_surveydata_expanded...")
    # This is where rows might get dropped
    df_expanded = get_surveydata_expanded(df_matched.copy(), Purpose.NADA)
    print(f"After get_surveydata_expanded: {len(df_expanded)} rows")
    print(f"Rows dropped: {len(df_matched) - len(df_expanded)}")

    # Check if our specific episode is in the expanded data
    if '160550' in str(df_expanded.get('PMSEpisodeID', pd.Series()).values):
        print("✓ Episode 160550 is present in expanded data")
    else:
        print("✗ Episode 160550 is MISSING from expanded data")

        # Check if it was in the original
        if '160550' in str(df_matched.get('PMSEpisodeID', pd.Series()).values):
            print("  (but it WAS in the original matched data)")

            # Find the row with this episode
            import pandas as pd
            episode_row = df_matched[df_matched['PMSEpisodeID'] == 160550]
            if not episode_row.empty:
                print(f"\n  Row details:")
                print(f"  - SLK: {episode_row.iloc[0].get('SLK')}")
                print(f"  - RowKey: {episode_row.iloc[0].get('RowKey')}")
                survey_data = episode_row.iloc[0].get('SurveyData', '')
                print(f"  - SurveyData length: {len(str(survey_data))}")
                print(f"  - SurveyData preview: {str(survey_data)[:200]}...")

                # Try to parse it
                from assessment_episode_matcher.utils.fromstr import clean_and_parse_json
                parsed = clean_and_parse_json(survey_data)
                print(f"  - Parsed type: {type(parsed)}")
                print(f"  - Is dict: {isinstance(parsed, dict)}")
        else:
            print("  (and it was NOT in the original matched data either)")

    print("\nTesting full prep_nada_fields...")
    df_prepped, warnings = prep_nada_fields(df_matched.copy(), config)
    print(f"After prep_nada_fields: {len(df_prepped)} rows")

    # Check if our specific episode is still there
    if '160550' in str(df_prepped.get('PMSEpisodeID', pd.Series()).values):
        print("✓ Episode 160550 is present in prepped data")
    else:
        print("✗ Episode 160550 is MISSING from prepped data")
