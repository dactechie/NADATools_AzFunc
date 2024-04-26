import logging
import pandas as pd
from mytypes import DataKeys as dk
from utils.df_ops_base import get_dupes_by_key, has_data

def get_mask_datefit(row, slack_days=7):
    # Create a Timedelta for slack days
    slack_td = pd.Timedelta(days=slack_days)
    # dk.assessment_date.value
    after_commencement = row['AssessmentDate'] >= (row['CommencementDate'] - slack_td)
    before_end_date = row['AssessmentDate'] <= (row['EndDate'] + slack_td)
    return after_commencement and before_end_date


def match_with_dates(ep_atom_df:pd.DataFrame, matching_ndays_slack: int):
    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    filtered_series = ep_atom_df.apply(get_mask_datefit, slack_days=matching_ndays_slack, axis=1)

    filtered_df = ep_atom_df[filtered_series]
    
    return filtered_df



def match_dates_increasing_slack(
      slk_program_matched:pd.DataFrame
      , max_slack:int=7):
  matching_ndays_slack = 0 
  asmt_key = dk.assessment_id.value
  ep_st_dt = dk.episode_start_date.value
  ep_ed_dt = dk.episode_end_date.value
  unmatched_asmt = slk_program_matched
  result_matched_dfs = []
  result_matched_df = pd.DataFrame()
  duplicate_rows_dfs = pd.DataFrame ()
  # atom_df['SLK_RowKey'] =  atom_df['SLK'] + '_' + atom_df['RowKey']
  # atom_df['SLK_RowKey'] =  atom_df['SLK'] + '_' + atom_df['RowKey']
  # ep_df['SLK_Program'] =  ep_df['SLK'] + '_' + ep_df['Program']

  # program_matched_slk_rks = slk_program_matched.SLK_RowKey

  while len(unmatched_asmt) > 0  and matching_ndays_slack <= max_slack:
      # Get matched assessments with the current slack
      matched_df = match_with_dates(unmatched_asmt, matching_ndays_slack)
      
      duplicate_rows_df = get_dupes_by_key(matched_df, asmt_key)
                                          #  'SLK_Program_y')
      # y because during the merge , assmt df is on the right (so not x)
      # this checks if there ATOMs matching to multiple episodes

      if has_data(duplicate_rows_df):
        #logging.error("Duplicate rows", duplicate_rows_df)
        duplicate_rows_dfs = pd.concat([duplicate_rows_dfs, duplicate_rows_df] , ignore_index=True)

      if len(matched_df) == 0: # no more SLK+Program matches between Episode and ATOM
         break
        # result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)
        # unmatched_by_date = unmatched_by_date[~unmatched_by_date.SLK_RowKey.isin(program_matched_slk_rks)]
        # return result_matched_df, unmatched_by_date, errors_warnings
      
      # Add the matched DataFrame to the list
      result_matched_dfs.append(matched_df)

      unmatched_asmt = unmatched_asmt[~unmatched_asmt[asmt_key].isin(matched_df[asmt_key])]

      ## there may be other assessments for this SLK that can match if the slack dways are increased
      ## don't exclude the SLK, but the SLK +RowKey

      # Increment the slack days for the next iteration
      matching_ndays_slack += 1

  if len(unmatched_asmt) > 0 :
     logging.info(f"There are still {len(unmatched_asmt)} unmatched ATOMs")
    #  logging.info(f"Unmatched by program: {len(unmatched_asmt.Program.value_counts())}")

    #  logger.info(f"There are still {len(unmatched_atoms)} unmatched ATOMs")
    #  logger.info(f"Unmatched by program: {len(unmatched_atoms.Program.value_counts())}")

  # Concatenate all matched DataFrames from the list
  if result_matched_dfs:
    result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)
  
  # add_to_issue_report(unmatched_by_date, IssueType.DATE_MISMATCH, IssueLevel.ERROR)
  mask_matched_eps = slk_program_matched.PMSEpisodeID.isin(result_matched_df.PMSEpisodeID)
  
  # in matching.main>merge_datasets, Episode is the 2nd param to pd.merge to Program_y
  unmatched_episodes = slk_program_matched[~mask_matched_eps] \
                        [['PMSEpisodeID', dk.client_id.value ,'Program_y',ep_st_dt,ep_ed_dt]].drop_duplicates()
  
  result_matched_df = result_matched_df.drop_duplicates(subset=[asmt_key])#overlapping episodes -e.g.same end date +start date
  
  
  # Can't do this because need all columns fro matching
  #unmatched_asmt = unmatched_asmt[['SLK','RowKey','AssessmentDate','Program','Staff','PDCSubstanceOfConcern']].drop_duplicates()

  #remove if matched to episodes #TODO TEST ME 
  # unmatched_asmt = unmatched_asmt[unmatched_asmt.PMSEpisodeID.isin(result_matched_df.PMSEpisodeID)]

  return result_matched_df, unmatched_asmt, duplicate_rows_dfs, unmatched_episodes
