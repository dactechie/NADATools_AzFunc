import pandas as pd
from mytypes import ValidationIssue

def make_validation_issue(row, vi1:ValidationIssue, unique_key):
  vi:ValidationIssue = vi1.make_copy()
  vi.key = row[unique_key]   
  return vi

def add_validation_issues(ew_df, vi:ValidationIssue, unique_key):
  if ew_df.empty:
    return []
  validation_issues = ew_df.apply(make_validation_issue,\
                                   axis=1, 
                                   args=(vi, unique_key)) \
                                   .tolist()
  

  return validation_issues