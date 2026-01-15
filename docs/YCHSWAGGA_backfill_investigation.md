# YCHSWAGGA Program Backfill Investigation

**Status:** Work in Progress
**Date:** 2026-01-15
**Establishment ID:** 12KK03112
**Program Code:** YCHSWAGGA

---

## Problem Statement

YCHSWAGGA program assessments have not been uploaded to the funder's NADAbase database. Investigation needed to identify and export all missing assessments.

---

## Summary of Findings

### Data Counts
| Metric | Count |
|--------|-------|
| Total ATOMs for YCHSWAGGA | 95 |
| Matched to episodes | 51 |
| Unmatched ATOMs | 44 |
| Episodes in MDS file | 34 |

### Unmatched Assessment Breakdown
| Type | Count | Reason |
|------|-------|--------|
| CLINICAL | 35 | Excluded by design (not NADA-reportable) |
| INTOUTC | 3 | SLK typos or missing episodes |
| REVOUTC | 6 | Date range issues or missing episodes |

### Root Causes Identified

1. **SLK Data Quality Issues (2 cases)**
   - `EBTOH111120042` in ATOM vs `EBTOH111120041` in MDS (last digit)
   - `LKYNA040920061` in ATOM vs `LKNYA040920061` in MDS (letter swap)

2. **Missing Episodes (2 cases)**
   - `RAELO160420021` - no episode in filtered MDS
   - `RISAL181120051` - no episode in filtered MDS

3. **Date Range Issues (5 cases)**
   - Assessments dated January 2026 but MDS only covers Oct-Dec 2025
   - Affected SLKs: CMRER041120042, ELYAC010920041, LCIOD050320051, OW2DW010920001
   - One assessment (EEIRA231120021 on 2025-12-18) should have matched

---

## Work Completed

### 1. Created Filtered MDS File
Filtered all MDS files from 2025_Q1 onwards for establishment 12KK03112.

**Output:** `NADASurveyGenerator\2026_Q1\MDS_12KK03112.csv` (34 episodes)

### 2. Ran Match and SurveyTxt APIs
```
/api/match?start_date=20251001&end_date=20251231&programs=YCHSWAGGA
→ 48 matched rows

/api/surveytxt?start_date=20251001&end_date=20251231&programs=YCHSWAGGA&include_all=true
→ 48 NADA records
```

**Output files in blob storage:**
- `20251001-20251231/forstxt_20251001-20251231_YCHSWAGGA_matched.csv`
- `20251001-20251231/surveytxt_20251001-20251231_YCHSWAGGA.csv`

### 3. Added `include_all` Parameter
New feature to include all assessments linked to episodes regardless of AssessmentDate.

**Commit:** `86cce61 feat: add include_all parameter to surveytxt API`

---

## Scripts Created

### 1. MDS Filtering Script
**Path:** `C:\Users\aftab.jalal\dev\NADATools_AzFunc\filter_mds_establishment.py`

Filters MDS files from quarterly folders for a specific establishment identifier.

**Usage:**
```python
# Edit these variables in the script:
TARGET_ESTABLISHMENT = "12KK03112"
QUARTERS = ["2025_Q1", "2025_Q2", "2025_Q3", "2025_Q4", "2026_Q1"]
```

### 2. Unmatched ATOMs Finder
**Path:** `C:\Users\aftab.jalal\dev\NADATools_AzFunc\find_unmatched_atoms.py`

Queries Azure Table Storage for all ATOMs of a program and identifies those not matched.

**Output:** `C:\Users\aftab.jalal\Downloads\YCHSWAGGA_unmatched_atoms.csv`

---

## Downloaded Files

Located in `C:\Users\aftab.jalal\Downloads\`:
- `12KK03112.csv` - Filtered MDS episodes
- `errors_warnings_YCHSWAGGA/` - Matching errors and warnings
- `forstxt_20251001-20251231_YCHSWAGGA_matched.csv` - Matched assessments
- `surveytxt_20251001-20251231_YCHSWAGGA.csv` - NADA export file
- `YCHSWAGGA_unmatched_atoms.csv` - List of unmatched ATOMs

---

## Next Steps

1. [ ] **Resolve SLK typos** - Correct data in either ATOM or MDS source systems
2. [ ] **Expand MDS date range** - Include earlier quarters to capture missing episodes
3. [ ] **Re-run matching** with corrected data to capture remaining assessments
4. [ ] **Upload surveytxt to NADAbase** - Submit to funder's portal

---

## Code Changes Made

### Files Modified (NADATools_AzFunc-program-filtering)
- `function_app.py` - Added `include_all` parameter to surveytxt endpoint
- `nada_helper.py` - Added `include_all_assessments` parameter throughout

### Commits
1. `ae7317d feat: add program filtering to match and surveytxt APIs`
2. `86cce61 feat: add include_all parameter to surveytxt API`
