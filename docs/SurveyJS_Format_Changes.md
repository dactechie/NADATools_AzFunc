# SurveyJS Format Changes Documentation

## Overview

This document details the changes to the SurveyJS data format that occurred on July 1, 2025, specifically focusing on how drug information is structured and how NADATools_AzFunc handles these changes.

## Data Format Changes

### Old Format (Pre-July 1, 2025)

In the old format, drug information was split into separate fields:

- **PDC (Primary Drug of Concern)**: Represented the primary substance of concern
- **ODC (Other Drugs of Concern)**: Represented additional substances of concern
- Drug usage data for substances like alcohol and cannabis was collected on a week-by-week basis (e.g., ATOPAlcoholWk1, ATOPCannabisWk4)
- Nicotine usage was collected only as a 4-week total

Example structure (simplified):
```json
{
  "PDC": "Alcohol",
  "ODC": ["Cannabis", "Nicotine"],
  "ATOPAlcoholWk1": 5,
  "ATOPAlcoholWk2": 3,
  "ATOPAlcoholWk3": 4, 
  "ATOPAlcoholWk4": 2,
  "ATOPCannabisWk1": 2,
  "ATOPCannabisWk2": 0,
  "ATOPCannabisWk3": 1,
  "ATOPCannabisWk4": 0,
  "ATOPNicotineTotal": 28
}
```

### New Format (Post-July 1, 2025)

In the new format, drug information uses a unified structure:

- **DrugsOfConcernDetails**: A structured field containing all drug information
- Still maintains week-by-week data collection for substances like alcohol and cannabis
- Nicotine continues to be collected as a 4-week total only

Example structure (simplified):
```json
{
  "DrugsOfConcernDetails": [
    {
      "drugName": "Alcohol",
      "isPrimary": true,
      "weeklyUsage": [5, 3, 4, 2]
    },
    {
      "drugName": "Cannabis",
      "isPrimary": false,
      "weeklyUsage": [2, 0, 1, 0]
    },
    {
      "drugName": "Nicotine",
      "isPrimary": false,
      "totalUsage": 28
    }
  ]
}
```

## NADATools_AzFunc Adaptations

### Configuration

The system maintains a mapping of drug categories in configuration.json:

```json
"drug_categories": {
  "Alcohol": ["Ethanol", "Alcohols, n.e.c."],
  "Cannabis": ["Cannabinoids and Related Drugs, n.f.d.", "Cannabinoids"],
  "Nicotine": ["Nicotine"]
  // Other categories...
}
```

### Processing Logic

The core of the data transformation is handled by the assessment_episode_matcher package:

1. The assessment data from SurveyJS is loaded into a dataframe
2. The `prep_nada_fields()` function in data_prep.py processes the data
3. For drug information:
   - The system detects whether the data is in the old format (PDC/ODC) or new format (DrugsOfConcernDetails)
   - For each drug category, it extracts the relevant data
   - For substances tracked weekly (alcohol, cannabis, etc.), it processes each week's data
   - For nicotine, it processes the total usage value

### Backward Compatibility

The commit message "updating matcher version to support mixed (old,new) drug info formats" (f73dfb4) indicates that the system was updated to handle both formats simultaneously. This allows for a smooth transition period where some data might be in the old format while newer data uses the new format.

## Nicotine/Tobacco Handling

A special consideration exists for nicotine/tobacco:

- Unlike other substances that are tracked on a week-by-week basis, nicotine is only captured as a 4-week total
- In the current implementation, "Nicotine" is used as a single category that covers both cigarette smoking and vaping
- The system is already configured to handle this as a specific drug category in configuration.json

## Testing Considerations

When testing the system, it's important to:

1. Verify that both old and new format data can be processed correctly
2. Ensure the weekly breakdown for tracked substances is accurate
3. Confirm that nicotine totals are correctly processed despite not having weekly data
4. Test with mixed data sources to ensure backward compatibility works as expected

## Related Files

- **configuration.json**: Contains the drug category definitions
- **nada_helper.py**: Handles the generation of NADA format data
- **assessment_episode_matcher/data_prep.py**: Contains the `prep_nada_fields()` function
- **assessment_episode_matcher/exporters/NADAbase.py**: Defines the output format for survey.txt