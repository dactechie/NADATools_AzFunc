from typing import Optional
import pandas as pd
from models.error_warnings import IssueLevel, IssueType, MatchingIssue \
  , EpisodeValidationMeta, AssessmentValidationMeta

# def create_ep_asmt_issues(data:pd.DataFrame 
#                 , issue_type:IssueType
#                 , issue_level:IssueLevel
#                 , msg:Optional[str]=""      
#                  ) -> list[MatchingIssue]:
  
#   if 'CommencementDate' in data.columns:
#     metas = [EpisodeValidationMeta(common_client_id=item.SLK
#                                     , program=item.Program
#                                 , episode_start=item.CommencementDate
#                                 , episode_end=item.EndDate)
#               for idx, item in data.iterrows()]
#   else:
#     metas = [AssessmentValidationMeta(common_client_id=item.SLK
#                                     , row_key=item.RowKey
#                            , assessment_date=item.AssessmentDate)
#              for idx, item in data.iterrows()]
  
#   issues = [MatchingIssue(meta=m, issue_type=issue_type
#                        ,issue_level=issue_level, msg=msg)
#             for m in metas]

#   return issues



# def create_assessment_issues(df:pd.DataFrame, i_type:IssueType,\
#             i_level:IssueLevel, msg:str="")-> list[MatchingIssue]:
#   metas = [AssessmentValidationMeta(common_client_id=item.SLK
#                                   , row_key=item.RowKey
#                                 , assessment_date=item.AssessmentDate)
#               for idx, item in df.iterrows()]

#   issues = [MatchingIssue(meta=m, issue_type=i_type
#                        ,issue_level=i_level, msg=msg)
#             for m in metas]
#   return issues

def gap_asesmtdate_epsd_boundaries(merged_df1:pd.DataFrame):
  merged_df = merged_df1.copy()
    # Calculating the difference in days
  merged_df.loc[:,'days_from_start'] = \
    (merged_df['AssessmentDate'] - merged_df['CommencementDate']).apply(lambda x: x.days)
    # (merged_df['AssessmentDate'] - merged_df['CommencementDate']) #.dt.days
  merged_df.loc[:,'days_from_end'] = \
    (merged_df['AssessmentDate'] - merged_df['EndDate']).apply(lambda x: x.days)
  return merged_df



def get_outofbounds_issues(unmatched_df:pd.DataFrame, limit_days:int = 1):
  gaps_df = gap_asesmtdate_epsd_boundaries(unmatched_df)
  # Warning if assessment is within 3 days outside the episode boundaries
  mask_isuetype_map = [ 
      {
        'mask': (gaps_df['days_from_start'] < 0) & \
                          (gaps_df['days_from_start'] >= -limit_days),
       'message': f"Assessment date is before episode start date by fewer than {limit_days}.",
       'issue_level':IssueLevel.WARNING
      },
      {
        'mask':  (gaps_df['days_from_end'] > 0) & \
                      (gaps_df['days_from_end'] <= limit_days),
        'message': f"Assessment date is after episode end date by fewer than {limit_days}.",
        'issue_level':IssueLevel.WARNING
      },
      {
        'mask': (gaps_df['days_from_start'] < -limit_days) ,
        'message': f"Assessment date is before episode start date by more than {limit_days}.",
        'issue_level':IssueLevel.ERROR
      },
      {
        'mask':  (gaps_df['days_from_end'] > limit_days) ,
        'message': f"Assessment date is after episode end date by more than {limit_days}.",
        'issue_level':IssueLevel.ERROR
      }
  ]

  results:list[MatchingIssue] = []
  for we in mask_isuetype_map:
    warns_errs = gaps_df[we['mask']]
    if len(warns_errs) > 0 :
      matching_issues = create_ep_asmt_issues(warns_errs
                   , issue_type=IssueType.DATE_MISMATCH
                   , issue_level=we['issue_level'], msg=we['message'])
      results.extend(matching_issues)
  return results


def get_duplicate_issues(dfs:list[pd.DataFrame]):
  i_t:IssueType = IssueType.ASMT_MATCHED_MULTI
  i_l:IssueLevel = IssueLevel.ERROR
  message = f"Assessment is matched to more than one episode."
  results:list[MatchingIssue] = []
  for df in dfs:
      matching_issues = create_assessment_issues(df
                   , i_type=i_t
                   , i_level=i_l, msg=message)
      results.extend(matching_issues)

  return results

def get_clientid_match_issues(dfs:list[pd.DataFrame],i_t:IssueType) -> list[MatchingIssue]:  
  i_l:IssueLevel = IssueLevel.ERROR
  if i_t==IssueType.CLIENT_ONLYIN_ASMT:
    message = f"SLK not found in Episodes"
  else:
    message = f"SLK not found in Assessments"
    
  results:list[MatchingIssue] = []
  for df in dfs:
      matching_issues = create_ep_asmt_issues(df
                   , issue_type=i_t
                   , issue_level=i_l, msg=message)
      results.extend(matching_issues)

  return results


def process_match_errors(date_unmatched_atoms
                         , errwrn_for_mergekey
                         , duplicate_rows_dfs
                         , warning_limit_days:int):
  all_ew = []
  if len(date_unmatched_atoms) > 0:
    date_matched_errwarns = get_outofbounds_issues(date_unmatched_atoms,\
                                                    limit_days=warning_limit_days)
    print('unmatched_atoms', date_unmatched_atoms)
    print ('errors_warnings_matchkey', errwrn_for_mergekey)
    # print ('date_matched_errwarns', date_matched_errwarns)
    has_error = True
    all_ew.extend(date_matched_errwarns)
  
  if len(duplicate_rows_dfs) > 0:
    duplicate_issues = get_duplicate_issues(duplicate_rows_dfs)
    # print('duplicate_issues', duplicate_issues)
    has_error = True
    all_ew.extend(duplicate_issues)

  return has_error, all_ew
   