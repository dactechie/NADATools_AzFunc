import logging
from datetime import date
import pandas as pd
# from utils.base import get_period_range
# from utils.io import write_parquet
from azutil.helper import get_fresh_data_only
from utils.environment import MyEnvironmentConfig
from matching.main import  filter_asmt_by_ep_programs, perform_date_matches, get_merged_for_matching, add_client_issues
from mytypes import DataKeys as dk
# from models.categories import Purpose
# from utils.df_xtrct_prep import prep_episodes #, extract_atom_data, cols_prep
# from utils.dtypes import blank_to_today_str, convert_to_datetime
from data_prep import prep_dataframe_nada, active_in_period
from importers import episodes as imptr_episodes
from importers import assessments as imptr_atoms
from exporters import NADAbase as out_exporter
import utils.df_ops_base as utdf
from mytypes import DataKeys as dk,\
      IssueType, IssueLevel

  # float_cols = df_final.select_dtypes(include=['float']).columns
  # df_final[float_cols] = df_final[float_cols].astype('Int64')
#  return df_final

# """
#   Cache to pq file if not
# """
# def extract_src_to_dfpq(data_type:DataType, use_proc_cache:bool):
#   source_folder = 'data/in/'
#   response_data_type = data_type

#   if use_proc_cache and DataType.EPISODES == data_type:
#     source_folder = 'data/processed/'
    
#     fname = f'{source_folder}MDS_1Jul2016-31Mar2024-AllPrograms.parquet'
#     response_data_type = DataType.PROCESSED_EPISODES

#   elif DataType.EPISODES == data_type:
#     full_period_str = '1Jul2016-31Mar2024'
#     fname =  f'{source_folder}MDS_1Jul2016-31Mar2024-AllPrograms.csv'

     
#   return fname, DataType.PROCESSED_EPISODES


def get_asmts_4_active_eps(episode_df:pd.DataFrame,\
                                        atoms_df:pd.DataFrame, 
                                        start_date:date,
                                          end_date:date,
                                      slack_ndays:int) -> tuple[pd.DataFrame, pd.DataFrame]:
  """
    Q: Why do we need to extract ATOMs before the reporting period?
    A: To ensure the stage-number is accurate. 

    1. Get all episodes that were active at any point during the period.
    2. To get the list of ATOMs active in the period, give the AssessmentDate range of:
        a. the start date of the earliest episode in step 1, minus n days for some slack.
        b. the end date of the reporting period.
    
      Important: 
      1. There may be ATOM assessments fro clients who are NOT in the list from step 1
      we return them anyway as the 'atoms_active_inperiod' and the validation steps later 
      would flag them.

      2. There may be no ATOM assessments in the reporting period, even though the matched episode
      had an active period (> say 28 days) in the reporting period.
  """

  ep_stfield, edfield = dk.episode_start_date.value, dk.episode_end_date.value  
  asmtdt_field = dk.assessment_date.value

  eps_active_inperiod =\
      active_in_period(episode_df,ep_stfield,edfield,
                       start_date, end_date)
  # active_clients_eps, eps_active_inperiod =\
  #     get_clients_for_eps_active_in_period(episode_df, start_date, end_date)
  
  # all_eps_4clients_w_active_eps_inperiod = episode_df[episode_df.SLK.isin(active_clients_eps)]
  
  
  #long running episodes should be eliminated as well.
  mask_within_ayear = (pd.to_datetime(eps_active_inperiod['EndDate']) - pd.to_datetime(eps_active_inperiod['CommencementDate'] )).dt.days <=366
  eps_active_inperiod = eps_active_inperiod.assign(within_one_year=mask_within_ayear)
  # eps_morethan_ayear = eps_active_inperiod[~mask_within_ayear]
  # logging.error(f"There are {len(eps_morethan_ayear)} episodes (active in reporting period) that were longer than a year")

  # eps_active_inperiod = eps_active_inperiod[mask_within_ayear]
  # logging.warning("Filtered out episodes if they were more than a year long")


  min_asmt_date = min(eps_active_inperiod[ep_stfield]) - pd.Timedelta(days=slack_ndays) # TODO: parameterize
  
  atoms_active_inperiod =\
      active_in_period(atoms_df,asmtdt_field, asmtdt_field,
                       min_asmt_date, end_date)
  
  # all_asmts_4clients_w_active_asmt_inperiod = atoms_df[atoms_df.SLK.isin(active_clients_asmt)]

  # active_clients_asmt =\
  #     get_clients_for_asmts_active_in_period(atoms_df, min_asmt_date, end_date)
  
  # all_asmts_4clients_w_active_asmt_inperiod = atoms_df[atoms_df.SLK.isin(active_clients_asmt)]

  # if atoms_active_inperiod.equals(all_asmts_4clients_w_active_asmt_inperiod):
  #   print("ATOMS OK")

  return atoms_active_inperiod, eps_active_inperiod


def write_validation_results(good_df:pd.DataFrame                            
                             , dates_ewdf:pd.DataFrame
                             , asmt_key_errors:pd.DataFrame
                             , ep_key_errors:pd.DataFrame
                             , asmt_key_warn:pd.DataFrame
                             , ep_key_warn:pd.DataFrame):
  ouput_folder = 'data/out/errors_warnings/'

  dates_ewdf.drop('SurveyData', axis=1, inplace=True)
  dates_ewdf.to_csv(f'{ouput_folder}dates_ewdf.csv')
  
  asmt_key_errors.drop('SurveyData', axis=1, inplace=True)
  asmt_key_errors.to_csv(f'{ouput_folder}asmt_key_errors.csv')
  
  ep_key_errors.to_csv(f'{ouput_folder}ep_key_errors.csv')
   
  asmt_key_warn.drop('SurveyData', axis=1, inplace=True)
  asmt_key_warn.to_csv(f'{ouput_folder}asmt_key_warn.csv')
  
  ep_key_warn.to_csv(f'{ouput_folder}ep_key_warn.csv')

# def filter_atoms_for_matching(min_date:date, max_date:date, atom_df:pd.DataFrame) -> pd.DataFrame:
#   min_asmt_date = min_date - pd.Timedelta(days=7)
#   max_asmt_date = max_date +  pd.Timedelta(days=7)
#   a_df = atom_df[( atom_df.AssessmentDate >= min_asmt_date ) & (atom_df.AssessmentDate <= max_asmt_date)]
#   return a_df


# def match_level_by_level(a_df1:pd.DataFrame, e_df:pd.DataFrame)-> pd.DataFrame:
#   a_df = a_df1.copy()
#   match_levels_mkey_groups = [ ['SLK','Program'], ['SLK']]
#   for mlevel_keys in match_levels_mkey_groups:
#     good_df, dates_ewdf, slk_program_ewdf, only_inas, only_inep = \
#       filter_good_bad(e_df, a_df
#                       , mergekeys_to_check=mlevel_keys
#                       , slack_ndays=env.matching_ndays_slack)
#     #a_df['date_matched'] = True # if it has an episode id then it is date matched
#     #unique key : SLK_RowqKey
#     mask = a_df.SLK_RowKey.isin(good_df['SLK_RowKey'])
#     a_df.loc[mask,'highest_level_matchkey'] = "_".join(mlevel_keys)
#   return a_df

"""
  #"Key" issues
    # Assessment issues: 
      # not matched to any episode, b/c:
         # assessment SLK not in any episode -> ERROR
         # SLK+Program not in any episode --> WARNING (keep in good dataset)
    # Episode issues:
      # zero assessments, b/c: (note: eps at end of reporting period may not have asmts)
        # episode SLK not in any assessment  -> ERROR 
        # SLK+Program not in Assessment-list -> WARNING (keep in good dataset)
"""        
def key_matching_errors(merge_key:str, slk_prog_onlyin:pd.DataFrame, it1:IssueType,
                         slk_onlyin:pd.DataFrame,  it2:IssueType):
  # slk_prog_onlyin (SLK+Program) doesn't need to have anytihng that is also in slk_onlyin (SLK) 
  # redundant
  slk_prog_onlyin1 = utdf.filter_out_common(slk_prog_onlyin, slk_onlyin, key=merge_key)
  # mask_common = slk_prog_onlyin[matchkey2].isin(slk_onlyin[matchkey2])  
  slk_prog_warn = slk_prog_onlyin1.assign(
                          issue_type = it1.value,
                          issue_level = IssueLevel.WARNING.value)

  slk_onlyin_error = slk_onlyin.assign(issue_type = it2.value,
                                         issue_level = IssueLevel.ERROR.value)
  # only_in_errors = pd.concat([slk_prog_new, slk_onlyin_new])

  return slk_onlyin_error, slk_prog_warn

  # slk_program_keys_ewdf = add_client_issues(slk_prog_onlyass, only_in_as_new)

  # slk_keys_ewdf = add_client_issues(only_in_ep2, slk_onlyinass)


def main2():
  MyEnvironmentConfig.setup('dev')
  env = MyEnvironmentConfig()
  reporting_start = date (2020,1,1)
  reporting_end =  date(2024,3,31)
  # source_folder = 'data/in/'
  eps_st = '20190701'  
  eps_end = '20240331'
  # eps_st ='20220101'
  # eps_end= '20240331'

  episode_df = imptr_episodes.import_data(eps_st, eps_end)
  print("Episodes shape " , episode_df.shape)
  
  # asmt_st, asmt_end = "20150101",  "20240411"
  asmt_st, asmt_end = "20190701",  "20240331"
  atom_df = imptr_atoms.import_data(asmt_st, asmt_end)
  # atom_df.to_csv('data/out/atoms.csv')
  print("ATOMs shape " , atom_df.shape)
  #FIXME: multiple atoms on the same day EACAR171119722 16/1/2024
  a_df, e_df = get_asmts_4_active_eps(episode_df, atom_df
                                      , start_date=reporting_start
                                      , end_date=reporting_end
                                      , slack_ndays=env.matching_ndays_slack)
  e_df.to_csv('data/out/active_episodes.csv')
  # a_df = filter_atoms_for_matching (min_date, max_date, atom_df)
  print("filtered ATOMs shape " , a_df.shape)
  print("filtered Episodes shape " , e_df.shape)

                  # SLK_RowKey
  a_df, asmt_key =  utdf.merge_keys( a_df, [dk.client_id.value, dk.per_client_asmt_id.value])

  a_ineprogs , a_notin_eprogs = filter_asmt_by_ep_programs (e_df, a_df)
  print(f"Assessments not in any of the programs of the episode {len(a_notin_eprogs)}")

  mkeys = ['SLK','Program']
  merged_df, merge_key, match_key, slk_prog_onlyinass, slk_prog_onlyin_ep= \
      get_merged_for_matching(e_df, a_ineprogs
                          ,mergekeys_to_check=mkeys
                          , match_keys=[dk.episode_id.value
                                                         , dk.assessment_id.value]
                          )
  good_df, dates_ewdf = perform_date_matches(merged_df
                                             , match_key
                                             , slack_ndays=env.matching_ndays_slack)
  # exclude already matched assessments
  # len(a_df) should be = len(merged_df) + len(slk_prog_onlyinass)

  # retry mismatching dates, with just SLK 
  # (in case the assesssment was made in a program different to the episode program)
  mkeys = ['SLK']
  # has to be an error - warnings (0-7 days) would have been matched
  # dtmtch_err_slkprog = dates_ewdf[dates_ewdf['issue_level']== IssueLevel.ERROR.value]
  # slk_prpg_unmatched = merged_df[merged_df[asmt_key].isin(dtmtch_err_slkprog[asmt_key].unique())]

 
  #when matching with just SLK, PMSEpisodeID is not present,
  # instead we have _x (from assessment) and _y from episode
  # we use the Episode's ID as the match Key
  # merged_df2,merge_key2, match_key2, slk_onlyinass, _= \
  #     get_merged_for_matching(e_df, slk_prpg_unmatched
  #                         ,mergekeys_to_check=mkeys
  #                         , match_keys=[dk.episode_id.value + '_y'
  #                                                        , dk.assessment_id.value])
  # good_df2, dates_ewdf2 = perform_date_matches(merged_df2
  #                                            , match_key2
  #                                            , slack_ndays=env.matching_ndays_slack)
  not_matched_asmts = a_ineprogs[~a_ineprogs[asmt_key].isin(good_df[asmt_key].unique())]
  # a_ineprogs , a_notin_eprogs = filter_asmt_by_ep_programs (e_df, not_matched_asmts)
  merged_df3,merge_key2, match_key3, slk_onlyinass, _ =  get_merged_for_matching(e_df
                                                                                  ,not_matched_asmts
                                                                                  ,mergekeys_to_check=mkeys
                                                                                  , match_keys=[dk.episode_id.value 
                                                         , dk.assessment_id.value])
  merged_df4 = merged_df3[merged_df3['Program_x'] != merged_df3['Program_y']]
  good_df2, dates_ewdf2 = perform_date_matches(merged_df4
                                             , match_key3
                                             , slack_ndays=env.matching_ndays_slack)  


  # program may be different for this client,asessment,  but highlight unlikely to be on the same day, so exclude :
  #(-8 - asmt date)
  good_df2['Ep_AsDate'] = good_df2['SLK']+'_'+ good_df2.PMSEpisodeID  +'_' + good_df2.PMSEpisodeID_SLK_RowKey.str[-8:] 
  good_df['Ep_AsDate'] = good_df['SLK']+'_'+ good_df.PMSEpisodeID  +'_' + good_df.PMSEpisodeID_SLK_RowKey.str[-8:]
  
  good_df2_v2 = utdf.filter_out_common(good_df2, good_df, key='Ep_AsDate')
  # good_df2_v2 =  good_df2[~good_df2.Ep_AsDate.isin(good_df.Ep_AsDate)]
  good_df2_v2 = utdf.drop_fields(good_df2_v2, ['Ep_AsDate'])

  #ESTBLISHmentID or Program_y has the right program (frm the matched episode)
  
  #can't use the result above (_)as we are only using the un-merged assesemtns (slk_prog_onlyinass) as input
  # slk_onlyin_ep = e_df[e_df.SLK.isin(a_inepregs.SLK.unique())]
  slk_onlyin_ep = utdf.filter_out_common(e_df, a_ineprogs, key='SLK')

  onlyin_amst_error, onlyin_amst_warn = key_matching_errors(
                      merge_key2, slk_prog_onlyinass
                      , IssueType.ONLY_IN_ASSESSMENT #warni
                      , slk_onlyinass 
                      ,IssueType.CLIENT_ONLYIN_ASMT) # error

  onlyin_ep_error, onlyin_ep_warn = key_matching_errors(
                      merge_key2, slk_prog_onlyin_ep
                      , IssueType.ONLY_IN_EPISODE
                      , slk_onlyin_ep 
                      ,IssueType.CLIENT_ONLYIN_EPISODE)


  #"Date match" issues
    # SLK+Program match but date not in +/- 7 days of ep boundaries
    # SLK match but date not in +/- 7 days of ep boundaries  
   
  final_good = pd.concat([good_df, good_df2_v2])
  
#one Assessment matching to multiple episodes    
    # duplicate_rows_df = get_dupes_by_key(matched_df, asmt_key)
  #eousides with no assessment in the reporting period
  # mask_matched_eps = ep_asmt_merged_df.PMSEpisodeID.isin(result_matched_df.PMSEpisodeID)

  # previously marked as errors, with the 2nd round of (relaxed i.e. SLK-only matching)
  # we mark them as warnings, as they matches were included in good_df2
  dates_ewdf.loc[dates_ewdf.SLK_RowKey.isin(final_good.SLK_RowKey),'issue_level'] = 1
  final_dates_ew = pd.concat([dates_ewdf, dates_ewdf2]).reset_index(drop=True)

  # TODO :Clients without a single ATOM in the period 
    #  part of an episode that has an active period in the reporting period
    #- before start of period : may have reported to NADA
    # after end of period :would report to NADA in the next period
  # TODO: see above : filter_atoms_for_matching and date_checks: asmt4clients_w_asmt_onlyoutof_period
  # in_period = good_df[(good_df.AssessmentDate >= reporting_start) & \
  #                     ( good_df.AssessmentDate <= reporting_end )]
  # inperiod_set = set(in_period.SLK.unique())
  # good_c_set = set(good_df.SLK.unique())
  
  # clients_w_asmts_only_outperiod =  good_df[good_df.SLK.isin(good_c_set - inperiod_set)]
  # print(clients_w_asmts_only_outperiod.SLK.unique())
  # # min(clients_w_zeroasmts_inperiod.AssessmentDate)

  df_reindexed = final_good.reset_index(drop=True)
  df_reindexed.to_csv('data/out/reindexed.csv', index_label="index")

  res, warnings_aod = prep_dataframe_nada(df_reindexed)
  # warnings_aod drug expand :  not typical_qty / typical_qty == 'Other' / not typical_unit:
  
  # res = res[~res.Program_y.isna()]
  # res.to_parquet('data/out/nada.parquet')

  # res.to_csv('data/out/nada.csv',index_label="index")
  # start_str, end_str = get_period_range(min_date, max_date)
  # write_result = write_parquet(res, f"data/out/nada_{start_str}-{end_str}.parquet")
  # res = pd.read_csv('data/out/nada.csv')


  st = out_exporter.generate_finaloutput_df(res)
  # st.to_parquet('/data/out/surveytxt.parquet')
  st.to_csv(f'data/out/{reporting_start}_{reporting_end}_surveytxt.csv', index=False)

  # write_validation_results(good_df, dates_ewdf, slk_program_keys_ewdf)
  write_validation_results(final_good, final_dates_ew
                           , onlyin_amst_error, onlyin_ep_error
                           , onlyin_amst_warn, onlyin_ep_warn)
  
  return st


def main ():
  MyEnvironmentConfig.setup('dev')
  # env = MyEnvironmentConfig()  
  # \2024-05-02T03:48:44.000Z


  results = get_fresh_data_only()
  max_timestamp = max(item["Timestamp"] for item in results)
  s = max_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") # type: ignore
  print(results)
  print("max time", s)
  

if __name__ == "__main__":
    res = main()

