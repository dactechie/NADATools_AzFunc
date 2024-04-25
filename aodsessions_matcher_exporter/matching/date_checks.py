import logging
import pandas as pd
import numpy as np
from  matching.mytypes import DataKeys as dk, \
  IssueType, ValidationError, ValidationWarning,\
       ValidationIssueTuple
import utils.df_ops_base as ut
import matching.validations as vd

def date_boundary_validators(limit_days:int) -> list[ValidationIssueTuple]:
  """
  Creates a list of dictionaries containing mask, message, and issue_level information.

  Args:
      limit_days (int): Limit for days before episode start date.

  Returns:
      list: A list of dictionaries representing the mask-message-level mapping.
  """  
  return [
    ValidationIssueTuple(
   
      mask = lambda df: (df['days_from_start'] < 0) & (df['days_from_start'] >= -limit_days),   
      validation_issue = ValidationWarning(      
                msg = f"Assessment date is before episode start date by fewer than {limit_days}.",
                issue_type = IssueType.DATE_MISMATCH,
      )
    ),
    ValidationIssueTuple(
       mask =  lambda df: (df['days_from_end'] > 0) &  (df['days_from_end'] <= limit_days),
      validation_issue = ValidationWarning(      
                msg = f"Assessment date is after episode end date by fewer than {limit_days}.",
                issue_type = IssueType.DATE_MISMATCH,
      )  
    ),
    ValidationIssueTuple(
       mask =   lambda df: df['days_from_start'] < -limit_days ,
      validation_issue = ValidationError(      
                msg = f"Assessment date is before episode start date by more than {limit_days}.",
                issue_type = IssueType.DATE_MISMATCH,
      )
  
    ),
    ValidationIssueTuple(
       mask =  lambda df: (df['days_from_end'] > limit_days) ,
      validation_issue = ValidationError(      
                msg =  f"Assessment date is after episode end date by more than {limit_days}.",
                issue_type = IssueType.DATE_MISMATCH,
      )
    ),
    
   
  ]


# Define matching functions
def assessment_date_validator(gaps_df, mit_dict:ValidationIssueTuple) ->\
                                tuple[pd.DataFrame, pd.DataFrame]:
  # Check if assessment date falls between commencement and end dates
  invalid_mask_lambda = mit_dict.mask # ~((df["commencement_date"] <= df["assessment_date"]) & (df["assessment_date"] <= df["end_date"]))
  if not invalid_mask_lambda:
      return gaps_df, pd.DataFrame()
  
  ew_df = gaps_df[invalid_mask_lambda(gaps_df.copy())]
  if not ut.has_data(ew_df):
     return gaps_df, pd.DataFrame()
  
  invalid_indices = ew_df.index.tolist()
  matched_df = gaps_df.drop(invalid_indices)

  return matched_df, ew_df


def gap_asesmtdate_epsd_boundaries(merged_df1:pd.DataFrame):
  merged_df = merged_df1.copy()
    # Calculating the difference in days
  merged_df.loc[:,'days_from_start'] = \
    (merged_df[dk.assessment_date.value] - merged_df[dk.episode_start_date.value]).apply(lambda x: x.days)    
  merged_df.loc[:,'days_from_end'] = \
    ( merged_df[dk.assessment_date.value] - merged_df[dk.episode_end_date.value]).apply(lambda x: x.days)
  return merged_df



def get_ep_boundary_issues(df:pd.DataFrame,  ukey:str) \
                      -> list: #tuple[list, pd.DataFrame, pd.DataFrame]:
   # some service type don't have assessments / look at the duration of episode
  vi = ValidationError(      
                msg =  f"No Assessment for episode.",
                issue_type = IssueType.NO_ASMT_IN_EPISODE)
  vis = vd.add_validation_issues(df, vi, ukey)
  return vis


def keep_nearest_mismatching_episode(unmatched_asmt:pd.DataFrame) -> pd.DataFrame:
   unm = unmatched_asmt.copy()
   unm['min_days'] = np.minimum(np.abs(unm['days_from_start']), np.abs(unm['days_from_end']))
   ew_df = unm.sort_values(['SLK_RowKey', 'min_days'])
   ew_df = ew_df.drop_duplicates('SLK_RowKey', keep='first')
   return ew_df

def get_assessment_boundary_issues(dt_unmtch_asmt:pd.DataFrame, mask_isuetypes:list[ValidationIssueTuple], ukey:str) \
                      -> tuple[list, pd.DataFrame]:
    gaps_df = gap_asesmtdate_epsd_boundaries(dt_unmtch_asmt)
    nearest_remaining_mismatch = keep_nearest_mismatching_episode(gaps_df)    
    validation_issues = []
    full_ew_df =  pd.DataFrame()
    for v in mask_isuetypes:
        nearest_remaining_mismatch, ew_df = assessment_date_validator(nearest_remaining_mismatch, v)
        
        if ut.has_data(ew_df):
            v.validation_issue
            vis = vd.add_validation_issues(ew_df, v.validation_issue, ukey)
            # print(vi)
            validation_issues.extend(vis)
            full_ew_df = pd.concat([full_ew_df, ew_df], ignore_index=True)
    # for v in mask_isuetypes:
    #   nearest_remaining_mismatch, ew_df = assessment_date_validator(nearest_remaining_mismatch, v)
    #   if ut.has_data(ew_df):
    #     full_ew_df = pd.concat([full_ew_df, ew_df], ignore_index=True)
    
    if len(nearest_remaining_mismatch) > 0:
      logging.warn("matched_df should not have anything remaining.")

    # full_ew_df = keep_nearest_mismatching_episode(full_ew_df)


    return validation_issues, full_ew_df

