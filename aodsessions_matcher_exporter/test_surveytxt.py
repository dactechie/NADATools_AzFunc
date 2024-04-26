import logging
from datetime import date
import pandas as pd
# from utils.base import get_period_range
# from utils.io import write_parquet
from utils.environment import MyEnvironmentConfig
from matching.main import filter_good_bad
from mytypes import DataKeys as dk
# from models.categories import Purpose
# from utils.df_xtrct_prep import prep_episodes #, extract_atom_data, cols_prep
# from utils.dtypes import blank_to_today_str, convert_to_datetime
from data_prep import prep_dataframe_nada, active_in_period
from importers import episodes as imptr_episodes
from importers import assessments as imptr_atoms
from exporters import NADAbase as out_exporter

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
  eps_morethan_ayear = eps_active_inperiod[~mask_within_ayear]
  logging.error(f"There are {len(eps_morethan_ayear)} episodes (active in reporting period) that were longer than a year")
  
  eps_active_inperiod = eps_active_inperiod[mask_within_ayear]
  logging.warn("Filtered out episodes if they were more than a year long")
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
                             , validation_issues:list
                             , dates_ewdf:pd.DataFrame
                             , slk_program_ewdf:pd.DataFrame):
  ouput_folder = 'data/out/errors_warnings/'
  vi = pd.DataFrame(validation_issues).drop_duplicates()
  vi.to_csv(f'{ouput_folder}validation_issues.csv')

  dates_ewdf.drop('SurveyData', axis=1, inplace=True)
  dates_ewdf.to_csv(f'{ouput_folder}dates_ewdf.csv')
  
  slk_program_ewdf.drop('SurveyData', axis=1, inplace=True)
  slk_program_ewdf.to_csv(f'{ouput_folder}slk_program_ewdf.csv')   

# def filter_atoms_for_matching(min_date:date, max_date:date, atom_df:pd.DataFrame) -> pd.DataFrame:
#   min_asmt_date = min_date - pd.Timedelta(days=7)
#   max_asmt_date = max_date +  pd.Timedelta(days=7)
#   a_df = atom_df[( atom_df.AssessmentDate >= min_asmt_date ) & (atom_df.AssessmentDate <= max_asmt_date)]
#   return a_df

def main2():
  MyEnvironmentConfig.setup('dev')
  env = MyEnvironmentConfig()
  reporting_start = date (2023,7,1)
  reporting_end =  date(2024,3,31)
  # source_folder = 'data/in/'
  eps_st = '20160701'
  eps_end = '20240331'
  episode_df = imptr_episodes.import_data(eps_st, eps_end)
  print("Episodes shape " , episode_df.shape)
  
  asmt_st, asmt_end = "20150101",  "20240411"
  atom_df = imptr_atoms.import_data(asmt_st, asmt_end)
  print("ATOMs shape " , atom_df.shape)

  a_df, e_df = get_asmts_4_active_eps(episode_df, atom_df
                                      , start_date=reporting_start
                                      , end_date=reporting_end
                                      , slack_ndays=env.matching_ndays_slack)
  # a_df = filter_atoms_for_matching (min_date, max_date, atom_df)
  print("filtered ATOMs shape " , a_df.shape)
  print("filtered Episodes shape " , e_df.shape)
 
  validation_issues, good_df, dates_ewdf, slk_program_ewdf = \
    filter_good_bad(e_df, a_df, slack_ndays=env.matching_ndays_slack)

  # TODO :Clients without a signal ATOM in the period
  in_period = good_df[(good_df.AssessmentDate >= reporting_start) & \
                      ( good_df.AssessmentDate <= reporting_end )]
  inperiod_set = set(in_period.SLK.unique())
  good_c_set = set(good_df.SLK.unique())
  
  clients_w_zeroasmts_inperiod =  good_df[good_df.SLK.isin(good_c_set - inperiod_set)]
  print(clients_w_zeroasmts_inperiod.SLK.unique())
  # min(clients_w_zeroasmts_inperiod.AssessmentDate)

  df_reindexed = good_df.reset_index(drop=True)

  res, warnings_aod = prep_dataframe_nada(df_reindexed)
  res = res[~res.Program_y.isna()]
  # res.to_parquet('/data/out/nada.parquet')

  # res.to_csv('data/out/nada.csv',index_label="index")
  # start_str, end_str = get_period_range(min_date, max_date)
  # write_result = write_parquet(res, f"data/out/nada_{start_str}-{end_str}.parquet")
  # res = pd.read_csv('data/out/nada.csv')
  # TODO : Converyt dates to ddmmyyyy

  st = out_exporter.generate_finaloutput_df(res)
  # st.to_parquet('/data/out/surveytxt.parquet')
  st.to_csv(f'data/out/{reporting_start}_{reporting_end}_surveytxt.csv', index=False)

  write_validation_results(good_df, validation_issues, dates_ewdf, slk_program_ewdf)
  
  return st


if __name__ == "__main__":
    res = main2()

