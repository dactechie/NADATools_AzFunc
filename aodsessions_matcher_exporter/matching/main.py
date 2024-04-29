import logging
import pandas as pd
from mytypes import DataKeys as dk,\
      IssueType, IssueLevel
      #  ValidationIssueTuple
import utils.df_ops_base as utdf
from utils import base as utbase
import matching.date_checks as dtchk

from matching import increasing_slack as mis

# from  matching.mytypes import ValidationIssue
def setup_df_for_check(episode_df: pd.DataFrame, \
                       assessment_df: pd.DataFrame,\
                          k_tup:list[str]) -> \
                          tuple[pd.DataFrame, pd.DataFrame, str]:
 
    # Get unique SLKs from episode and assessment dataframes
    # fixme: is a copy necessary?
    ep_df = episode_df.copy()
    as_df = assessment_df.copy()    

    if len(k_tup) > 1:
        ep_df, key = utdf.merge_keys(episode_df, k_tup)
        as_df, _ = utdf.merge_keys(assessment_df, k_tup)
    else:
        key = k_tup[0]
  
    return ep_df, as_df, key


# TODO: refactor with df_ops_base.get_lr_mux_unmatched
def merge_check_keys(episode_df: pd.DataFrame, assessment_df: pd.DataFrame, k_tup: list[str]):
    
    epdf_mkey, asdf_mkey, key = setup_df_for_check(episode_df,assessment_df, k_tup)
    
    only_in_ep, only_in_as, in_both = utbase.check_if_exists_in_other(
       set(epdf_mkey[key]),   set(asdf_mkey[key])
    )
    if only_in_as:  #len(assessment_df[asdf_mkey[key].isin(only_in_as)] )
      logging.info(f" (mergkey:{key}) only in assessment: {len(only_in_as)}")
    if only_in_ep: # len(episode_df[epdf_mkey[key].isin(only_in_ep)])
      logging.info(f"(mergkey:{key})  only in episode  {len(only_in_ep)}")

    return   epdf_mkey[epdf_mkey[key].isin(only_in_ep)] \
                 , asdf_mkey[asdf_mkey[key].isin(only_in_as)] \
                 , epdf_mkey[epdf_mkey[key].isin(in_both)]\
                 , asdf_mkey[asdf_mkey[key].isin(in_both)], \
                  key



def merge_datasets(episode_df:pd.DataFrame, assessment_df:pd.DataFrame, common_cols:list[str], match_keys:list[str]):
    """
      Inner join on "Common_Cols"
      and also return a new key which is the merge of fields in "keys_merge"
    """
    # Merge the two dataframes based on SLK, Program, and client_type
    # TODO extract, "client_type" from SurveyData
    merged_df = pd.merge(assessment_df,\
                          episode_df, on=common_cols,how="inner")
    merged_df, unique_key = utdf.merge_keys( merged_df, match_keys)

    # print ("Merged", merged_df)
    return merged_df, unique_key




# def get_ep_boundary_issues(df:pd.DataFrame,  ukey:str) \
#                       -> list: #tuple[list, pd.DataFrame, pd.DataFrame]:
#    # some service type don't have assessments / look at the duration of episode
#   vi = ValidationError(      
#                 msg =  f"No Assessment for episode.",
#                 issue_type = IssueType.NO_ASMT_IN_EPISODE)
#   vis = vd.add_validation_issues(df, vi, ukey)
#   return vis

def perform_date_matches(merged_df: pd.DataFrame, match_key:str, slack_ndays:int):
    
    # include all the warnings in the good_Df using matching with increasing slack
    result_matched_df, dt_unmat_asmts, duplicate_rows_dfs = \
      mis.match_dates_increasing_slack (merged_df #,mergekeys_to_check
                                          , max_slack=slack_ndays)

    mask_isuetype_map = dtchk.date_boundary_validators(limit_days=slack_ndays)
    # validation_issues, matched_df, invalid_indices =
    ew_df = dtchk.get_assessment_boundary_issues(\
       dt_unmat_asmts, mask_isuetype_map, match_key)
    
    # unmatched_eps_df = unmatched_eps_df.assign(issue_type=IssueType.NO_ASMT_IN_EPISODE.value
    #                         , issue_level=IssueLevel.ERROR.value)
    # duplicate_rows_dfs = duplicate_rows_dfs.assign(issue_type=IssueType.ASMT_MATCHED_MULTI.value
    #                         , issue_level=IssueLevel.ERROR.value)
    # final_dates_ewdf = pd.concat([ew_df, duplicate_rows_dfs],ignore_index=True)

    # return validation_issues, good_df, ew_df
    return  result_matched_df, ew_df
    

# def perform_date_matches(merged_df: pd.DataFrame, unique_key:str, slack_ndays:int):
    
#     # include all the warnings in the good_Df using matching with increasing slack
#     result_matched_df, dt_unmat_asmts, duplicate_rows_dfs, unmatched_eps_df = \
#       mis.match_dates_increasing_slack (merged_df , max_slack=slack_ndays)

#     mask_isuetype_map = dtchk.date_boundary_validators(limit_days=slack_ndays)
#     # validation_issues, matched_df, invalid_indices =
#     validation_issues, ew_df = dtchk.get_assessment_boundary_issues(\
#        dt_unmat_asmts, mask_isuetype_map, unique_key)
    
#     vis = dtchk.get_ep_boundary_issues(unmatched_eps_df, dk.episode_id.value)
#     if vis:
#       validation_issues.extend(vis)
    
#     # DO THe same thing for duplicate rows
#     vis_dupe = dtchk.get_ep_boundary_issues(duplicate_rows_dfs, dk.episode_id.value)
#     if vis_dupe:
#       validation_issues.extend(vis_dupe)

#     # print(f"\n\n \t\t\tbut returning {result_matched_df.head()}")
#     # print(validation_issues)
#     # return validation_issues, good_df, ew_df
#     return validation_issues, result_matched_df, ew_df
    

"""
  Inter Dataset Matching Keys (IDMK) - Assessment & Episodes  : SLK + Program + ClientType
            Purpose:  Before checking for Date matches, we need to link  the two dataset something common

  Dataset Unique IDs (DUID) :
           Assesment :  SLK+RowKey
           Episode   : EpisodeID or if absent SLK + Program + CommencementDate + ClientType

      Purpose: Uniquely identify a record in eith

"""
def add_client_issues(only_in_ep, only_in_as):
    df_list = []
    
    if utdf.has_data(only_in_ep):
        only_in_ep_copy = only_in_ep.copy()
        only_in_ep_copy['issue_type'] = IssueType.CLIENT_ONLYIN_EPISODE
        df_list.append(only_in_ep_copy)
    
    if utdf.has_data(only_in_as):
        only_in_as_copy = only_in_as.copy()
        only_in_as_copy['issue_type'] = IssueType.CLIENT_ONLYIN_ASMT
        df_list.append(only_in_as_copy)
    
    if df_list:
        full_ew_df = pd.concat(df_list, ignore_index=True)
    else:
        full_ew_df = pd.DataFrame()
    
    return full_ew_df
     
# def add_client_issues(only_in_ep, only_in_as, mkey):
#   full_ew_df = pd.DataFrame()
#   validation_issues = []
#   if utdf.has_data(only_in_ep):
#     vi_only_in_ep = ValidationError(
#                 msg = f"Only in Episode.",
#                 issue_type = IssueType.CLIENT_ONLYIN_EPISODE)
#     vis = vd.add_validation_issues(only_in_ep, vi_only_in_ep, mkey)        
#     validation_issues.extend(vis)        
#     full_ew_df = only_in_ep #pd.concat([full_ew_df, only_in_ep] , ignore_index=True)

#   if utdf.has_data(only_in_as):
#     vi_only_in_as = ValidationError(
#                 msg = f"Only in Assessment.",
#                 issue_type = IssueType.CLIENT_ONLYIN_ASMT)
#     vis = vd.add_validation_issues(only_in_as, vi_only_in_as, mkey)
#     full_ew_df = pd.concat([full_ew_df, only_in_as] , ignore_index=True)
#     validation_issues.extend(vis)
#   return validation_issues, full_ew_df


# def clientassessments_by_program_relation(a_df: pd.DataFrame, allowed_programs) -> tuple:
#     """
#     Categorize client assessments based on their relation to the specified NADAPrograms.

#     Args:
#         a_df (pd.DataFrame): The input DataFrame containing client assessments.
#         NADAPrograms (ndarray): The list of NADA programs to compare against.

#     Returns:
#         tuple: A tuple containing two DataFrames:
#             - a_df_related: Assessments for clients where all programs are in NADAPrograms.
#             - a_df_unrelated: Assessments for clients where at least one program is not in NADAPrograms.
#     """
#     # Group 1: Identify SLKs having one or more non-NADA assessments
#     # SLK: DEF may only have TSS programs, so DEF would be in this list
#     # SLK:ABC may have a TSS and a NADA program -  so ABC would be in this list  due to the TSS asmt
#     slks_w_nonNADA_asmt = a_df.loc[~a_df.Program.isin(allowed_programs), 'SLK'].unique()

#     # Group 2: Identify SLKs where all assessments are in NADAPrograms.
#     # From the original asmt list, match SLKs where it does NOT match the 
#     #           first group (i.e. having one or more non-NADA assessments)
#     slks_all_NADA = a_df.loc[~a_df.SLK.isin(slks_w_nonNADA_asmt), 'SLK'].unique()

#     # Filter assessments for clients related to NADAPrograms
#     a_df_related = a_df[a_df.SLK.isin(slks_all_NADA)]

#     # Filter assessments for clients with at least one unrelated program
#     a_df_unrelated = a_df[a_df.SLK.isin(slks_w_nonNADA_asmt)]

#     return a_df_related, a_df_unrelated

def filter_asmt_by_ep_programs(
        ep_df: pd.DataFrame, a_df:pd.DataFrame)\
            -> tuple[pd.DataFrame, pd.DataFrame]:
  ep_programs = ep_df['Program'].unique()
  aprog_in_any_eprog =a_df['Program'].isin(ep_programs)
  a_df_epprog = a_df[aprog_in_any_eprog]
  return a_df_epprog, a_df[~aprog_in_any_eprog]
   

def get_merged_for_matching(episode_df: pd.DataFrame
                            , assessment_df: pd.DataFrame
                            , mergekeys_to_check:list[str]
                            ):

    #ew_df - Errors Warnings Dataframe
    # ewdf = pd.DataFrame()
    # 1. Remove records with keys not common to both assessments and episodes: 
    #   (so we report the correct mismatch type and don't try to date-match them)
    only_in_ep, only_in_as, ep_df_inboth, as_df_inboth, merge_key = merge_check_keys(
        episode_df, assessment_df, k_tup=mergekeys_to_check# SLK or SLK+Program
    )
    # if any(only_in_ep) or any(only_in_as):
    #   # if they are irrelevent programs (TSS/Coco when doing NADA), we don't want to report them as errors
    #   # only_in_as_ep_prog = filter_asmt_by_ep_programs(only_in_as, ep_df_inboth)
    #   ewdf = add_client_issues(only_in_ep, only_in_as)
      
    # 2. Match for assessment date within episodes dates
    merged_df, match_key = merge_datasets(ep_df_inboth
                                           , as_df_inboth
                                           , common_cols=mergekeys_to_check
                                           , match_keys=[dk.episode_id.value
                                                         , dk.assessment_id.value])
    return merged_df, merge_key, match_key, only_in_as, only_in_ep
                                                        
"""
    # before Prepare for  date matching 
    #clients may move in and out of CoCo/Arcadia, don;t want to report those as errors, so don't bother matching them
    # as_df_inboth = filter_asmt_by_ep_programs(as_df_inboth, ep_df_inboth)  -> done by caller
"""
# def filter_good_bad(merged_df: pd.DataFrame
#                    # , merge_key:str                 
#                     , match_key:str
#                     , slack_ndays:int) -> tuple[pd.DataFrame
#                                                 , pd.DataFrame
#                                            ]:


#     good_df, dates_ewdf = perform_date_matches(merged_df
#                                              #  , merge_key  # sLK or SLK+Program  
#                                               , match_key #epid_slk_rowkey
#                                               , slack_ndays=slack_ndays)

#     return good_df, dates_ewdf

#     # TODO: collect all errors and warnings
    # return errors, warnings, and good dataset



# def filter_good_bad(episode_df: pd.DataFrame
#                     , assessment_df: pd.DataFrame
#                     , slack_ndays:int) -> tuple[list, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#     validation_issues = []
#     slk_program_ewdf = pd.DataFrame()
#     fl_clntid = dk.client_id.value

#     keys_to_check = [(fl_clntid,), (fl_clntid, "Program")]  # with client_type

#     # 1. check for SLK in both datasets.
#     only_in_ep, only_in_as, ep_df_inboth, as_df_inboth, mkey = check_keys(
#         episode_df, assessment_df, k_tup=keys_to_check[0]
#     )
#     if any(only_in_ep) or any(only_in_as):
#       only_in_as_ep_prog = filter_asmt_by_ep_programs(only_in_as, ep_df_inboth)
#       vis, slk_program_ewdf = add_client_issues(only_in_ep, only_in_as_ep_prog, mkey)
#       validation_issues.extend(vis)

#    # 2. check for SLK +Program in both datasets.

#     #clients may move in and out of CoCo/Arcadia, don;t want to report those as errors
#     as_df_inboth_ep_prog = filter_asmt_by_ep_programs(as_df_inboth, ep_df_inboth)
      
#     # # we only want to match against clients which who had all programs in list of episodes' programs
#     ### as_df_inboth, asdf_nonnada_only  = clientassessments_by_program_relation(as_df_inboth
#     ## #                                            , ep_df_inboth['Program'].unique())
#     ##non_epprog_clients = len(asdf_nonnada_only[dk.client_id.value].unique())
#     ##logging.info(f"There were {len(asdf_nonnada_only)} assessments ({non_epprog_clients} clients),\
#     ##              who had ATOMs only in programs different from epispde-programs.")

#     # only_in_ep, only_in_as, ep_df_inboth_prg, as_df_inboth_prg, mkey = check_keys(
#     #     ep_df_inboth, as_df_inboth_ep_prog, k_tup=keys_to_check[1]
#     # )
#     # if any(only_in_ep) or any(only_in_as):
#     #   vis, ew_df = add_client_issues(only_in_ep, only_in_as, mkey)
#     #   validation_issues.extend(vis)
#     #   if utdf.has_data(ew_df):    
#     #      slk_program_ewdf =  pd.concat([slk_program_ewdf, ew_df] , ignore_index=True)

#     #TODO: for the in_both , do the time-boundaries check
#     # as_df_inboth_prg, asmt_key =  utdf.merge_keys( as_df_inboth_prg, [fl_clntid, dk.per_client_asmt_id.value])
#     # merged_df, unique_key = merge_datasets(ep_df_inboth_prg, as_df_inboth_prg
#     #                                        ,
#     #                                        common_cols=[fl_clntid,"Program"]
#     #                                        , keys_merge=[dk.episode_id.value, asmt_key]) #IDMK
    
#     as_df_inboth, asmt_key =  utdf.merge_keys( as_df_inboth_ep_prog, [fl_clntid, dk.per_client_asmt_id.value])
#     merged_df, unique_key = merge_datasets(ep_df_inboth, as_df_inboth
#                                            ,
#                                            common_cols=[fl_clntid]
#                                            , keys_merge=[dk.episode_id.value, asmt_key]) #IDMK

#     date_validation_issues, good_df, dates_ewdf = perform_date_matches(merged_df
#                                                                        , unique_key
#                                                                        , slack_ndays=slack_ndays)
#     if utdf.has_data(dates_ewdf):
#         validation_issues.extend(date_validation_issues)
#         # full_ew_df = pd.concat([full_ew_df, dates_ew_df], ignore_index=True)
#     # dates_ewdf[dates_ewdf.PMSEpisodeID_SLK_RowKey == date_validation_issues[0].key]
#     return validation_issues, good_df, dates_ewdf, slk_program_ewdf

#     # TODO: collect all errors and warnings
#     # return errors, warnings, and good dataset


# # # from test_data import episode_data, assessment_data
# def main():
#     # episode_df = pd.DataFrame(episode_data)
#     # assessment_df = pd.DataFrame(assessment_data)
#     episode_df = pd.read_csv('data/in/TEST_NSWMDS.csv')
#     assessment_df = pd.read_csv('data/in/TEST_ATOM.csv')
#     validation_issues, good_df, ew_df = filter_good_bad(episode_df, assessment_df)
#     ew_df.to_csv('data/out/ew_df.csv')
   


# if __name__ == "__main__":
#     main()
