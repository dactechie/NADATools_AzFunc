import logging
from datetime import date, timedelta
import pandas as pd
from mytypes import DataKeys as dk, IssueLevel, IssueType, Purpose, ValidationIssue
# from utils.environment import MyEnvironmentConfig, ConfigKeys
import utils.df_ops_base as utdf
from utils import base as utbase
import matching.date_checks as dtchk
from matching import increasing_slack as mis



def get_data_for_matching(ep_imptr, asmt_imptr, eps_st, eps_end
                          , reporting_start, reporting_end
                          , assessment_start, assessment_end
                          , slack_for_matching:int, refresh:bool=True) \
                            -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

  episode_df = ep_imptr.import_data(eps_st, eps_end)
  if not utdf.has_data(episode_df):
      logging.error("No episodes")
      return pd.DataFrame(), pd.DataFrame(),pd.DataFrame(), pd.DataFrame()
  print("Episodes shape ", episode_df.shape)

  atom_df = asmt_imptr.import_data(assessment_start
                                   , assessment_end
                                   , purpose=Purpose.NADA
                                   , refresh=refresh
            )
  # atom_df.to_csv('data/out/atoms.csv')
  print("ATOMs shape ", atom_df.shape)
  # FIX ME: multiple atoms on the same day EACAR171119722 16/1/2024

   #, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom 
  a_df, e_df,inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom  = get_asmts_4_active_eps2(
                episode_df, atom_df, start_date=reporting_start
              , end_date=reporting_end, slack_ndays=slack_for_matching)
  
  # a_df = filter_atoms_for_matching (min_date, max_date, atom_df)
  # vis = [] 
  # for asmt in inperiod_atomslk_notin_ep:
  #    v = ValidationIssue("In Period ATOM SLK not in Episode",
  #                        issue_type=IssueType.INPERIOD_ASMTSLK_NOTIN_EP,
  #                        issue_level=IssueLevel.WARNING)
  #   # ValidationIssue

  print("filtered ATOMs shape ", a_df.shape)
  print("filtered Episodes shape ", e_df.shape)
  return a_df, e_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom




#remove unrelated SLKs bfore the reporting period

def get_asmts4clients_w_asmts_inperiod(atoms_df: pd.DataFrame,
                           minpos_asmt_date: date,
                           start_date: date,
                           end_date: date):
    """
      Remove any assessments for clients(SLKs) who are found ONLY before the reporting period.
      The resulting df may have assessment before the reporting period, as long as they have 
      at least one assessment in reporting period
    """
    f_asmt_dt = dk.assessment_date.value
    slk = dk.client_id

    atoms_active_inperiod =\
      utdf.in_period(atoms_df, f_asmt_dt, f_asmt_dt,
                         start_date, end_date) 
    asmts_beforeperiod = utdf.in_period(atoms_active_inperiod
                                                          , f_asmt_dt, f_asmt_dt
                                                          , minpos_asmt_date
                                                          , start_date - timedelta(days=1))
    slks_beforeperiod = set(asmts_beforeperiod[[slk]]) #.unique()
    atoms_active_inperiod = set(atoms_active_inperiod[[slk]])

    slks_beforeperiod_notalso_inperiod = slks_beforeperiod - atoms_active_inperiod
    mask_slks_onlyb4_period = atoms_df[slk].isin(slks_beforeperiod_notalso_inperiod)
    
    slks = set(atoms_df[mask_slks_onlyb4_period][slk])

    logging.info(f"SLKs only before period : {slks} ")
    return atoms_df[~mask_slks_onlyb4_period]


def get_asmts_4_active_eps(episode_df: pd.DataFrame,
                           atoms_df: pd.DataFrame,
                           start_date: date,
                           end_date: date,
                           slack_ndays: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    #, pd.DataFrame, pd.DataFrame]:
    """
      Q: Why do we need to extract ATOMs before the reporting period?
      A: To ensure the stage-number is accurate. 

      1. Get all episodes that were active at any point during the period.
      2. To get the list of ATOMs active in the period, give the AssessmentDate range of:
          a. the start date of the earliest episode in step 1, minus n days for some slack.
          b. the end date of the reporting period.

        Important: 
        1. There may be ATOM assessments for clients who are NOT in the list from step 1
        we return them anyway as the 'atoms_active_inperiod' and the validation steps later 
        would flag them.

        2. There may be no ATOM assessments in the reporting period, even though the matched episode
        had an active period (> say 28 days) in the reporting period.
    """

    f_ep_st, f_ep_ed = dk.episode_start_date.value, dk.episode_end_date.value
    # f_asmt_dt = dk.assessment_date.value
    # slk = dk.client_id

    eps_active_inperiod = utdf.in_period(episode_df, f_ep_st
                                           , f_ep_ed
                                           , start_date, end_date)
    #NOTE: Do this one year later , it is irrelevant here
    mask_within_ayear = (pd.to_datetime(eps_active_inperiod['EndDate']) -
                          pd.to_datetime(
                           eps_active_inperiod['CommencementDate'])).dt.days <= 366
    eps_active_inperiod = eps_active_inperiod.assign(
                            within_one_year=mask_within_ayear)
    
    minpos_asmt_date = min(
        eps_active_inperiod[f_ep_st]) - pd.Timedelta(days=slack_ndays)

    atoms_active_inperiod =\
        get_asmts4clients_w_asmts_inperiod(atoms_df, minpos_asmt_date,
                         start_date, end_date)
    
    common_slk_atom_mask = atoms_active_inperiod.SLK.isin(
        eps_active_inperiod.SLK)
    atoms_slk_not_in_ep = atoms_active_inperiod[~common_slk_atom_mask]
    #inperiodatom_slknot_inep = active_in_period(
    #    atoms_slk_not_in_ep, f_asmt_dt, f_asmt_dt, start_date, end_date)
    

    commonslk_ep_mask = eps_active_inperiod.SLK.isin(atoms_active_inperiod.SLK)
    ep_slk_not_in_atom = eps_active_inperiod[~commonslk_ep_mask]
    #inperiodep_slk_not_inatom = active_in_period(
    #    ep_slk_not_in_atom, f_ep_st, f_ep_ed, start_date, end_date)

    return atoms_active_inperiod[common_slk_atom_mask], eps_active_inperiod[commonslk_ep_mask]
      #, \
      #  inperiodatom_slknot_inep, inperiodep_slk_not_inatom


def get_asmts_4_active_eps2(episode_df: pd.DataFrame,
                           atoms_df: pd.DataFrame,
                           start_date: date,
                           end_date: date,
                           slack_ndays: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
      Q: Why do we need to extract ATOMs before the reporting period?
      A: To ensure the stage-number is accurate. 

      1. Get all episodes that were active at any point during the period.
      2. To get the list of ATOMs active in the period, give the AssessmentDate range of:
          a. the start date of the earliest episode in step 1, minus n days for some slack.
          b. the end date of the reporting period.

        Important: 
        1. There may be ATOM assessments for clients who are NOT in the list from step 1
        we return them anyway as the 'atoms_active_inperiod' and the validation steps later 
        would flag them.

        2. There may be no ATOM assessments in the reporting period, even though the matched episode
        had an active period (> say 28 days) in the reporting period.
    """

    ep_stfield, edfield = dk.episode_start_date.value, dk.episode_end_date.value
    asmtdt_field = dk.assessment_date.value

    eps_active_inperiod =\
        utdf.in_period(episode_df, ep_stfield, edfield,
                         start_date, end_date)
    # active_clients_eps, eps_active_inperiod =\
    #     get_clients_for_eps_active_in_period(episode_df, start_date, end_date)

    # all_eps_4clients_w_active_eps_inperiod = episode_df[episode_df.SLK.isin(active_clients_eps)]

    # long running episodes should be eliminated as well.
    mask_within_ayear = (pd.to_datetime(eps_active_inperiod['EndDate']) - pd.to_datetime(
        eps_active_inperiod['CommencementDate'])).dt.days <= 366
    eps_active_inperiod = eps_active_inperiod.assign(
        within_one_year=mask_within_ayear)
    # eps_morethan_ayear = eps_active_inperiod[~mask_within_ayear]
    # logging.error(f"There are {len(eps_morethan_ayear)} episodes (active in reporting period) that were longer than a year")

    # eps_active_inperiod = eps_active_inperiod[mask_within_ayear]
    # logging.warning("Filtered out episodes if they were more than a year long")

    # TODO: parameterize
    min_asmt_date = min(
        eps_active_inperiod[ep_stfield]) - pd.Timedelta(days=slack_ndays)

    atoms_active_inperiod =\
        utdf.in_period(atoms_df, asmtdt_field, asmtdt_field,
                         min_asmt_date, end_date)

    common_slk_atom_mask = atoms_active_inperiod.SLK.isin(
        eps_active_inperiod.SLK)
    atoms_slk_not_in_ep = atoms_active_inperiod[~common_slk_atom_mask]
    inperiodatom_slknot_inep = utdf.in_period(
        atoms_slk_not_in_ep, asmtdt_field, asmtdt_field, start_date, end_date)
    print("Inperiod atoms , SLK not in episde", set(inperiodatom_slknot_inep.loc[:,'SLK']))

    commonslk_ep_mask = eps_active_inperiod.SLK.isin(atoms_active_inperiod.SLK)
    ep_slk_not_in_atom = eps_active_inperiod[~commonslk_ep_mask]
    inperiodep_slk_not_inatom = utdf.in_period(
       ep_slk_not_in_atom, ep_stfield, edfield, start_date, end_date)
    print("Inperiod episode , SLK not in ATOM", set(inperiodep_slk_not_inatom.loc[:,'SLK']))

    return atoms_active_inperiod[common_slk_atom_mask], eps_active_inperiod[commonslk_ep_mask], \
        inperiodatom_slknot_inep, inperiodep_slk_not_inatom



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
                            , match_keys:list[str]
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
                                           , match_keys=match_keys)
    return merged_df, merge_key, match_key, only_in_as, only_in_ep
                                                        
