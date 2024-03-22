import pandas as pd
from .mytypes import DataKeys as dk,\
      IssueType, ValidationError
      #  ValidationIssueTuple
import utils.df_ops_base as utdf
from utils import base as utbase
import matching.date_checks as dtchk
import matching.validations as vd

# from  matching.mytypes import ValidationIssue

mismatch_slack_limit:int = 4

def setup_df_for_check(episode_df: pd.DataFrame, \
                       assessment_df: pd.DataFrame,\
                          k_tup: tuple) -> \
                          tuple[pd.DataFrame, pd.DataFrame, str]:
    key:str
    # Get unique SLKs from episode and assessment dataframes
    ep_df = episode_df.copy()
    as_df = assessment_df.copy()    
    if isinstance(k_tup, tuple):
        keys = list(k_tup)
        if len(keys) > 1:
            ep_df, key = utdf.merge_keys(episode_df, keys)
            as_df, _ = utdf.merge_keys(assessment_df, keys)
        else:
            key = k_tup[0]
    else:
        key = k_tup
    return ep_df, as_df, key


# TODO: refactor with df_ops_base.get_lr_mux_unmatched
def check_keys(episode_df: pd.DataFrame, assessment_df: pd.DataFrame, k_tup: tuple):
    
    epdf_mkey, asdf_mkey, key = setup_df_for_check(episode_df,assessment_df, k_tup)
    
    only_in_ep, only_in_as,in_both = utbase.check_if_exists_in_other(
       set(epdf_mkey[key]),   set(asdf_mkey[key])
    )
    if only_in_as:
        print("oly in assessment", assessment_df[asdf_mkey[key].isin(only_in_as)])
    if only_in_ep:
        print("oly in episode", episode_df[epdf_mkey[key].isin(only_in_ep)])

    return   epdf_mkey[epdf_mkey[key].isin(only_in_ep)] \
                 , asdf_mkey[asdf_mkey[key].isin(only_in_as)] \
                 , epdf_mkey[epdf_mkey[key].isin(in_both)]\
                 , asdf_mkey[asdf_mkey[key].isin(in_both)], \
                  key



def merge_datasets(episode_df:pd.DataFrame, assessment_df:pd.DataFrame, lr_cols:list[str]):
    # Merge the two dataframes based on SLK, Program, and client_type
    # TODO extract, "client_type" from SurveyData
    merged_df = pd.merge(assessment_df,\
                          episode_df, on=[dk.client_id.value,\
                                          "Program"],how="inner")
    merged_df, unique_key = utdf.merge_keys( merged_df, lr_cols)

    # print ("Merged", merged_df)
    return merged_df, unique_key


from matching import increasing_slack as mis
# def match_dates_increasing_slack(
#       slk_program_matched:pd.DataFrame
#       , max_slack:int=7):
def perform_date_matches(merged_df: pd.DataFrame, unique_key:str):
    
    # include all the warnings in the good_Df using matching with increasing slack
    result_matched_df, dt_unmat_asmts, duplicate_rows_dfs, unmatched_eps_df = \
      mis.match_dates_increasing_slack (merged_df , max_slack=7)

    mask_isuetype_map = dtchk.date_boundary_validators(limit_days=7)
    # validation_issues, matched_df, invalid_indices =
    validation_issues, ew_df = dtchk.get_assessment_boundary_issues(\
       dt_unmat_asmts, mask_isuetype_map, unique_key)
    
    vis = dtchk.get_ep_boundary_issues(unmatched_eps_df, dk.episode_id.value)
    if vis:
      validation_issues.extend(vis)
    
    # DO THe same thing for duplicate rows
    vis_dupe = dtchk.get_ep_boundary_issues(duplicate_rows_dfs, dk.episode_id.value)
    if vis_dupe:
      validation_issues.extend(vis_dupe)

    print(f"\n\n \t\t\tbut returning {result_matched_df.head()}")
    print(validation_issues)
    # return validation_issues, good_df, ew_df
    return validation_issues, result_matched_df, ew_df
    

    

"""
  Inter Dataset Matching Keys (IDMK) - Assessment & Episodes  : SLK + Program + ClientType
            Purpose:  Before checking for Date matches, we need to link  the two dataset something common

  Dataset Unique IDs (DUID) :
           Assesment :  SLK+RowKey
           Episode   : EpisodeID or if absent SLK + Program + CommencementDate + ClientType

      Purpose: Uniquely identify a record in eith

"""

def add_client_issues(only_in_ep, only_in_as, mkey):
  full_ew_df = pd.DataFrame()
  validation_issues = []
  if utdf.has_data(only_in_ep):
    vi_only_in_ep = ValidationError(
                msg = f"Only in Episode.",
                issue_type = IssueType.CLIENT_ONLYIN_EPISODE)
    vis = vd.add_validation_issues(only_in_ep, vi_only_in_ep, mkey)        
    validation_issues.extend(vis)        
    full_ew_df = only_in_ep #pd.concat([full_ew_df, only_in_ep] , ignore_index=True)
  if utdf.has_data(only_in_as):
    vi_only_in_as = ValidationError(
                msg = f"Only in Assessment.",
                issue_type = IssueType.CLIENT_ONLYIN_ASMT)
    vis = vd.add_validation_issues(only_in_as, vi_only_in_as, mkey)
    full_ew_df = pd.concat([full_ew_df, only_in_as] , ignore_index=True)
    validation_issues.extend(vis)
  return validation_issues, full_ew_df


def filter_good_bad(episode_df: pd.DataFrame, assessment_df: pd.DataFrame):
    validation_issues = []
    slk_program_ewdf = pd.DataFrame()

    keys_to_check = [(dk.client_id.value,)]#, (dk.client_id.value, "Program")]  # with client_type

    # 1. check for SLK in both datasets.
    only_in_ep, only_in_as, ep_df_inboth, as_df_inboth, mkey = check_keys(
        episode_df, assessment_df, k_tup=keys_to_check[0]
    )
    if any(only_in_ep) or any(only_in_as):
      vis, slk_program_ewdf = add_client_issues(only_in_ep, only_in_as, mkey)
      validation_issues.extend(vis)

    # 2. check for SLK +Program in both datasets.
    # only_in_ep, only_in_as, ep_df_inboth, as_df_inboth, mkey = check_keys(
    #     ep_df_inboth, as_df_inboth, k_tup=keys_to_check[1]
    # )
    # if any(only_in_ep) or any(only_in_as):
    #   vis, ew_df = add_client_issues(only_in_ep, only_in_as, mkey)
    #   validation_issues.extend(vis)
    #   if utdf.has_data(ew_df):
    #      slk_program_ewdf =  pd.concat([slk_program_ewdf, ew_df] , ignore_index=True)      

    #TODO: for the in_both , do the time-boundaries check
    as_df_inboth, asmt_key =  utdf.merge_keys( as_df_inboth, [dk.client_id.value, dk.per_client_asmt_id.value])
    
    merged_df, unique_key = merge_datasets(ep_df_inboth, as_df_inboth, lr_cols=[dk.episode_id.value,
                                                                                asmt_key]) #IDMK
    date_validation_issues, good_df, dates_ewdf = perform_date_matches(merged_df, unique_key)
    if utdf.has_data(dates_ewdf):
        validation_issues.extend(date_validation_issues)
        # full_ew_df = pd.concat([full_ew_df, dates_ew_df], ignore_index=True)

    return validation_issues, good_df, dates_ewdf, slk_program_ewdf

    # TODO: collect all errors and warnings
    # return errors, warnings, and good dataset


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
