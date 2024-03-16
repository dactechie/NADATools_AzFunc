import pandas as pd
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




def all_date_validations(df:pd.DataFrame, mask_isuetypes:list[ValidationIssueTuple], ukey:str) \
                      -> tuple[list, pd.DataFrame, pd.DataFrame]:
    gaps_df = gap_asesmtdate_epsd_boundaries(df)
    matched_df = gaps_df
    validation_issues = []
    full_ew_df =  pd.DataFrame()
    for v in mask_isuetypes:
        matched_df, ew_df = assessment_date_validator(matched_df, v)
        
        if ut.has_data(ew_df):
            v.validation_issue
            vis = vd.add_validation_issues(ew_df, v.validation_issue, ukey)
            # print(vi)
            validation_issues.extend(vis)
            full_ew_df = pd.concat([full_ew_df, ew_df], ignore_index=True)
            
    return validation_issues, matched_df, full_ew_df
