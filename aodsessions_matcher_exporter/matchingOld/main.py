import pandas as pd


# from utils.fromstr import convert_format_datestr
from data_prep import prep_dataframe_nada 
from matching.matching import match_dates_increasing_slack\
                      , match_by_matching_keys, match_by_clientid
from models.error_warnings import IssueType, MatchingIssue
                      #,prep_assmt_4match 
from warnings_errors import  process_match_errors, get_clientid_match_issues
from data_config import nada_final_fields
from models.categories import Purpose, ResultType



def get_matched_assessments( ep_df, atom_df, purpose:Purpose
                            , episode_boundary_slack_days:int=7
                            , warning_limit_days:int=3) -> tuple[pd.DataFrame|None
                                                                 , list, bool] :
        
    #log len(ep_df), min(ep_df.CommencementDate), max(ep_df.CommencementDate)
    # ep_df = prep_episodes(episode_data)

   
    # period_start_dt, period_end_dt = get_firststart_lastend(ep_df['CommencementDate']
    #                                                         , ep_df['EndDate'])

    # get ATOMs from DB
    # atom_df, is_processed = extract_atom_data(period_start_dt, period_end_dt
    #                                 , purpose=purpose)# NADA->NSW Programs only
    # if atom_df is None or atom_df.empty:
    #    return atom_df, [], True, False
    
    # if not is_processed:
      
  
    # atom_df = prep_assmt_4match(atom_df)
        
    # prep for match


    # do matching
    # note anomalies
      # - no atoms for episode
      # - no episodes for atom
    
    errwrn_atom, errwrn_ep, slk_matched_atoms, slk_matched_eps =\
          match_by_clientid(ep_df, atom_df,client_id='SLK')
    


    slk_program_merged, errwrn_for_mergekey = \
         match_by_matching_keys(slk_matched_eps
                                , slk_matched_atoms, matching_keys=['SLK', 'Program'])
    
    result_matched_df, date_unmatched_atoms, duplicate_rows_dfs = \
        match_dates_increasing_slack(slk_program_merged, episode_boundary_slack_days)
    

    mis_slk_atom:list[MatchingIssue] = get_clientid_match_issues(errwrn_atom, IssueType.CLIENT_ONLYIN_ASMT)
    get_clientid_match_issues(errwrn_ep, IssueType.CLIENT_ONLYIN_EPISODE)

    has_error, all_ew = process_match_errors(date_unmatched_atoms
                                             , errwrn_for_mergekey
                                             , duplicate_rows_dfs
                                             , warning_limit_days)
    # get_stage_per_episode
    # cols_prep
    
    return result_matched_df, all_ew, has_error#, is_processed



# if __name__ == '__main__':
#       get_matched_assessments
#       if purpose == Purpose.NADA and not is_processed:
#       atom_df = prep_dataframe_nada(atom_df)

#   pass
  # get_matched_assessments(episode_data, purpose=Purpose.NADA)
  # get_matched_assessments(episode_data, purpose=Purpose.NSW)
  # get_matched_assessments(episode_data, purpose=Purpose.NADA, episode_boundary_slack_days=30)
  # get_matched_assessments(episode_data, purpose=Purpose.NADA, episode_boundary_slack_days=30, warning_limit_days=2)
  # get_matched_assessments(episode_data, purpose=Purpose.NADA, episode_boundary_slack_days=30, warning_limit_days=2)
  # get_matched_assessments(episode_data, purpose=Purpose.NADA, episode_boundary_slack_days=30, warning_limit_days=2)'
