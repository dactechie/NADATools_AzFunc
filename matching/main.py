import pandas as pd
from .mytypes import DataKeys as dk, ValidationIssue,\
 IssueType, ValidationError, ValidationWarning,\
       ValidationIssueTuple
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
    if isinstance(k_tup, tuple):
        keys = list(k_tup)
        if len(keys) > 1:
            ep_df, key = utdf.merge_keys(episode_df, keys)
            as_df, _ = utdf.merge_keys(assessment_df, keys)
        else:
            keys = k_tup[0]
    else:
        ep_df = episode_df.copy()
        as_df = assessment_df.copy()
        key = k_tup
    return ep_df, as_df, key


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



def merge_datasets(episode_df, assessment_df, lr_cols=\
                   [dk.episode_id.value,dk.assessment_id.value]):
    # Merge the two dataframes based on SLK, Program, and client_type
    # TODO extract, "client_type" from SurveyData
    merged_df = pd.merge(assessment_df,\
                          episode_df, on=[dk.client_id.value,\
                                          "Program"],how="inner")
    merged_df, unique_key = utdf.merge_keys( merged_df, lr_cols)

    print ("Merged", merged_df)
    return merged_df, unique_key


def check_dates(merged_df: pd.DataFrame, unique_key:str):
    

    mask_isuetype_map = dtchk.date_boundary_validators(limit_days=mismatch_slack_limit)
    # validation_issues, matched_df, invalid_indices =
    validation_issues, good_df, ew_df = dtchk.all_date_validations(merged_df, mask_isuetype_map, unique_key)
    print(validation_issues)
    return validation_issues, good_df, ew_df

    

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
    full_ew_df = pd.DataFrame()

    keys_to_check = [(dk.client_id.value), (dk.client_id.value, "Program")]  # with client_type
    only_in_ep, only_in_as, ep_df_inboth, as_df_inboth, mkey = check_keys(
        episode_df, assessment_df, k_tup=keys_to_check[0]
    )
    if any(only_in_ep) or  any(only_in_as):
      vis, ew_df = add_client_issues(only_in_ep, only_in_as, mkey)
      validation_issues.extend(vis)
      full_ew_df = pd.concat([full_ew_df, ew_df] , ignore_index=True)

    only_in_ep, only_in_as, ep_df_inboth, as_df_inboth, mkey = check_keys(
        ep_df_inboth, as_df_inboth, k_tup=keys_to_check[1]
    )
    if any(only_in_ep) or  any(only_in_as):
      vis, ew_df = add_client_issues(only_in_ep, only_in_as, mkey)
      validation_issues.extend(vis)
      full_ew_df = pd.concat([full_ew_df, ew_df] , ignore_index=True)
    # vis, ew_df2 = add_client_issues(only_in_ep, only_in_as, mkey)
    # validation_issues.extend(vis)
    # full_ew_df = pd.concat([full_ew_df, ew_df2] , ignore_index=True)
    

    #TODO: for the in_both , do the time-boundaries check
    merged_df, unique_key = merge_datasets(ep_df_inboth, as_df_inboth) #IDMK
    date_validation_issues, good_df, dates_ew_df = check_dates(merged_df, unique_key)
    if utdf.has_data(dates_ew_df):
        validation_issues.extend(date_validation_issues)
        full_ew_df = pd.concat([full_ew_df, dates_ew_df], ignore_index=True)
        
    return validation_issues, good_df, full_ew_df

    # TODO: collect all errors and warnings
    # return errors, warnings, and good dataset


# from test_data import episode_data, assessment_data
# def main():
#     episode_df = pd.DataFrame(episode_data)
#     assessment_df = pd.DataFrame(assessment_data)
#     validation_issues, good_df, ew_df = filter_good_bad(episode_df, assessment_df)
   


# if __name__ == "__main__":
#     main()
