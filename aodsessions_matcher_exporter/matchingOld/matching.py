
import logging
import pandas as pd
from utils.df_ops_base import get_dupes_by_key, merge_keys

from warnings_errors import   IssueLevel,\
         IssueType, process_match_errors


def get_mask_datefit(row, slack_days=7):
    # Create a Timedelta for slack days
    slack_td = pd.Timedelta(days=slack_days)

    after_commencement = row['AssessmentDate'] >= (row['CommencementDate'] - slack_td)
    before_end_date = row['AssessmentDate'] <= (row['EndDate'] + slack_td)
    return after_commencement and before_end_date


def match_with_dates(ep_atom_df, matching_ndays_slack: int):
    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    mask = ep_atom_df.apply(get_mask_datefit, slack_days=matching_ndays_slack, axis=1)

    filtered_df = ep_atom_df[mask]
    
    return filtered_df


# TODO: CLIENT_ONLYIN_ASMT, CLIENT_ONLYIN_EP

def not_in_source_df(source2_df:pd.DataFrame
                     , source_df:pd.DataFrame
                    #  , vm:Type[ValidationMeta]
                     , issue_type:IssueType
                     ,  key:str):
  source2_clients:list[str] = list(source2_df[key].unique())
  in_source2 = source_df[source_df[key].isin(source2_clients)] 
  not_in_source2 = source_df[~source_df[key].isin(source2_clients)] 
  # not_in_source2 = source_df.loc[~in_source2.index] #Operator "~" not supported for type "Index[Unknown]"

  if not_in_source2:
    errors_warnings = create_ep_asmt_issues(not_in_source2
                                  # , vm
                                  , issue_type
                                  , issue_level=IssueLevel.ERROR)
  return errors_warnings, in_source2

# from models.error_warnings import IssueLevel, IssueType, MatchingIssue \
#   , EpisodeValidationMeta, AssessmentValidationMeta

def match_by_clientid(ep_df, atom_df, client_id:str='SLK'):
  errwrn_atom, slk_matched_atoms = not_in_source_df(ep_df
                                      , atom_df
                                      , issue_type=IssueType.CLIENT_ONLYIN_ASMT                          
                                      , key=client_id)
  errwrn_ep, slk_matched_eps = not_in_source_df( atom_df,ep_df
                                          , issue_type=IssueType.CLIENT_ONLYIN_EPISODE
                                          , key=client_id)
  return errwrn_atom, errwrn_ep, slk_matched_atoms, slk_matched_eps


def match_by_matching_keys(ep_df, atom_df, matching_keys=['SLK', 'Program']):
    
  atom_df = merge_keys(atom_df,  merge_fields=['SLK', 'RowKey']) 
  ep_df = merge_keys(ep_df, merge_fields=['SLK', 'Program'])

  slk_program_merged = pd.merge(ep_df, atom_df, how='inner'
                                , left_on=matching_keys, right_on=matching_keys)
  
  # not_in_source_atom = atom_df[~atom_df['SLK_RowKey'].isin(slk_program_merged['SLK_RowKey'])]
  # AssessmentValidationMeta(common_client_id=not_in_source_atom.iloc[0,'']
  #                          ,row_key=  )

  errors_warnings_atom, _ = not_in_source_df(slk_program_merged
                                          , atom_df
                                          , issue_type=IssueType.ONLY_IN_ASSESSMENT                           
                                          , key='SLK_RowKey')

  
  errors_warnings_ep, _ = not_in_source_df(slk_program_merged
                                          , ep_df
                                          , issue_type=IssueType.ONLY_IN_EPISODE
                                          , key='SLK_Program')
  

    
  errors_warnings_atom.extend(errors_warnings_ep)
  return slk_program_merged, errors_warnings_atom


def match_dates_increasing_slack(
      slk_program_matched:pd.DataFrame
      , max_slack:int=7):
  matching_ndays_slack = 0 
  unmatched_by_date = slk_program_matched
  result_matched_dfs = []
  result_matched_df = pd.DataFrame()
  duplicate_rows_dfs:list[pd.DataFrame] = []
  # atom_df['SLK_RowKey'] =  atom_df['SLK'] + '_' + atom_df['RowKey']
  # atom_df['SLK_RowKey'] =  atom_df['SLK'] + '_' + atom_df['RowKey']
  # ep_df['SLK_Program'] =  ep_df['SLK'] + '_' + ep_df['Program']

  # program_matched_slk_rks = slk_program_matched.SLK_RowKey

  while len(unmatched_by_date) > 0  and matching_ndays_slack <= max_slack:
      # Get matched assessments with the current slack
      matched_df = match_with_dates(unmatched_by_date, matching_ndays_slack)
      
      duplicate_rows_df = get_dupes_by_key(matched_df, 'SLK_RowKey')

      if not (duplicate_rows_df is None or duplicate_rows_df.empty):      
        #logging.error("Duplicate rows", duplicate_rows_df)
        duplicate_rows_dfs.append(duplicate_rows_df)

      if len(matched_df) == 0: # no more SLK+Program matches between Episode and ATOM
         break
        # result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)
        # unmatched_by_date = unmatched_by_date[~unmatched_by_date.SLK_RowKey.isin(program_matched_slk_rks)]
        # return result_matched_df, unmatched_by_date, errors_warnings
      
      # Add the matched DataFrame to the list
      result_matched_dfs.append(matched_df)

      unmatched_by_date = unmatched_by_date[~unmatched_by_date.SLK_RowKey.isin(matched_df.SLK_RowKey)]

      ## there may be other assessments for this SLK that can match if the slack dways are increased
      ## don't exclude the SLK, but the SLK +RowKey

      # Increment the slack days for the next iteration
      matching_ndays_slack += 1

  if len(unmatched_by_date) > 0 :
     logging.info(f"There are still {len(unmatched_by_date)} unmatched ATOMs")
     logging.info(f"Unmatched by program: {len(unmatched_by_date.Program.value_counts())}")
    #  logger.info(f"There are still {len(unmatched_atoms)} unmatched ATOMs")
    #  logger.info(f"Unmatched by program: {len(unmatched_atoms.Program.value_counts())}")

  # Concatenate all matched DataFrames from the list
  if result_matched_dfs:
    result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)
  
  # add_to_issue_report(unmatched_by_date, IssueType.DATE_MISMATCH, IssueLevel.ERROR)
  return result_matched_df, unmatched_by_date, duplicate_rows_dfs

