import os
import logging
from typing import Optional

from assessment_episode_matcher.configs import load_blob_config
from assessment_episode_matcher.utils.environment import ConfigKeys
# from assessment_episode_matcher.setup.bootstrap import Bootstrap
from assessment_episode_matcher.importers.main import BlobFileSource, FileSource
from assessment_episode_matcher.importers import episodes as EpisodesImporter
from assessment_episode_matcher.importers import assessments as ATOMsImporter
from assessment_episode_matcher.utils.fromstr import get_date_from_str
from assessment_episode_matcher.utils.dtypes import date_to_str

from assessment_episode_matcher.matching import main as match_helper
from assessment_episode_matcher.matching.errors import process_errors_warnings

from assessment_episode_matcher.exporters.main import AzureBlobExporter
import assessment_episode_matcher.utils.df_ops_base as utdf
from assessment_episode_matcher.mytypes import DataKeys as dk, Purpose
from assessment_episode_matcher.configs.constants import MatchingConstants


def get_essentials(container_name:str|None, qry_params:dict) -> tuple[dict,dict]:
  if not (qry_params and qry_params.get('s') and qry_params.get('e')):
    msg = f"unable to proceed without start and end dates."
    logging.exception(msg , stack_info=True, exc_info=True)
    return {}, {"error": msg }
   
  if not container_name:
      msg = f"unable to proceed without app config {ConfigKeys.AZURE_BLOB_CONTAINER.value}"
      logging.exception(msg , stack_info=True, exc_info=True)
      return {}, {"error": msg }
  
  config = load_blob_config(container_name)
  if not config:
      msg = f"No configuration.json file (blob-container: {container_name})."
      logging.info(msg)

  
  return config, {}


def run(start_yyyymmd:str, end_yyyymmd:str
        , only_for_slks: Optional[list[str]]
        , get_nearest_matching_slk:Optional[int] = 0
        ) -> dict:

  container_name = os.environ.get('AZURE_BLOB_CONTAINER',"")
  
  config, errors = get_essentials(container_name
                                  , qry_params={'s':start_yyyymmd
                                                , 'e':end_yyyymmd})
  if errors:
    return errors
  
  # override loaded config, if passed in from URL
  if get_nearest_matching_slk:
     config[MatchingConstants.GET_NEAREST_SLK] = 1
  
  logging.info(f"Get nearest SLK : {config.get(MatchingConstants.GET_NEAREST_SLK,0)}.")

  result = match_store_results(
              reporting_start_str=start_yyyymmd
              ,reporting_end_str = end_yyyymmd 
              , container_name = container_name
              , config=config
              , only_for_slks=only_for_slks)
  return result


def match_store_results(reporting_start_str:str, reporting_end_str:str                         
                        ,container_name:str
                        , config:dict
                        , only_for_slks: Optional[list[str]]
                        ) -> dict:

    ep_folder, asmt_folder = "MDS", "ATOM"
    p_str = f"{reporting_start_str}-{reporting_end_str}"
    limited_slks = ""

    slack_for_matching = 7# int(cfg.get(ConfigKeys.MATCHING_NDAYS_SLACK.value, 7))
    # print(Bootstrap.config)
    logging.info(ConfigKeys.MATCHING_NDAYS_SLACK.value)

    reporting_start, reporting_end = get_date_from_str (reporting_start_str,"%Y%m%d") \
                                      , get_date_from_str (reporting_end_str,"%Y%m%d")

    ep_file_source:FileSource = BlobFileSource(container_name=container_name
                                            , folder_path=ep_folder)

    episode_df, ep_cache_to_path = EpisodesImporter.import_data(
                            reporting_start_str, reporting_end_str
                            , ep_file_source
                            , prefix=ep_folder, suffix="AllPrograms", config=config)
    if not utdf.has_data(episode_df):
      logging.error("No episodes")
      return {"result":"no episode data"}

    if only_for_slks:
       limited_slks ="_filtered-slks_"
       episode_df = episode_df[episode_df['SLK'].isin(only_for_slks)]
       
       
    # year_ago = reporting_start - timedelta(days=365)
    # atoms_start_yrago_str = str(date_to_str(year_ago))
    min_epcommence_date = min(episode_df.CommencementDate)
    atoms_start = str(date_to_str(min(min_epcommence_date, reporting_start)))
    atom_file_source:FileSource = BlobFileSource(container_name=container_name
                                            , folder_path=asmt_folder)
    atoms_df, atom_cache_to_path = ATOMsImporter.import_data(
                            atoms_start, reporting_end_str
                            , atom_file_source
                            , prefix=asmt_folder, suffix="AllPrograms"
                            , purpose=Purpose.NADA, config=config
                            , only_for_slks=only_for_slks
                            , refresh=True)
    
    # if atom_cache_to_path:
    #   exp = AzureBlobExporter(container_name=atom_file_source.container_name) #
    #   exp.export_dataframe(data_name=f"{asmt_folder}/{atom_cache_to_path}.parquet", data=atoms_df)    
    #                         # , prefix="MDS", suffix="AllPrograms")
    if not utdf.has_data(atoms_df):
      logging.error("No ATOMs")
      return {"result":"no ATOM data"}
    
    a_df, e_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom = \
      match_helper.get_data_for_matching2(episode_df, atoms_df
                                        , min_epcommence_date, reporting_end, slack_for_matching=7)    
    if not utdf.has_data(a_df) or not utdf.has_data(e_df):
        logging.warning("No data to match. Ending")
        return {"result":"No Data to match." }
    # e_df.to_csv('data/out/active_episodes.csv')
    final_good, ew = match_helper.match_and_get_issues(e_df, a_df
                                          , inperiod_atomslk_notin_ep
                                          , inperiod_epslk_notin_atom
                                          , slack_for_matching
                                          , reporting_start, reporting_end
                                          , config)

    warning_asmt_ids  = final_good.SLK_RowKey.unique()
      
    ae = AzureBlobExporter(container_name=atom_file_source.container_name
                           ,config={'location' : f"{p_str}/errors_warnings{limited_slks}"})    
    ew_stats = process_errors_warnings(ew, warning_asmt_ids, dk.client_id.value
                            , period_start=reporting_start
                            , period_end=reporting_end
                            , audit_exporter=ae)
  

    df_reindexed = final_good.reset_index(drop=True)

    exp = AzureBlobExporter(container_name=atom_file_source.container_name) #
    
    exp.export_dataframe(data_name=f"{p_str}/forstxt_{p_str}_matched{limited_slks}.csv"
                    , data=df_reindexed)

    return {"num_matched_rows": len(df_reindexed),
            "audit_stats": ew_stats
            }