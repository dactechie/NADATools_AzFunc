import os
import logging
import pandas as pd
from assessment_episode_matcher.configs import load_blob_config
from assessment_episode_matcher.utils.environment import ConfigKeys
from assessment_episode_matcher.exporters.main import AzureBlobExporter
from assessment_episode_matcher.data_prep import prep_dataframe_nada
from assessment_episode_matcher.exporters import NADAbase as nada_df_generator
from assessment_episode_matcher.importers.main import  BlobFileSource
import assessment_episode_matcher.utils.df_ops_base as utdf
import assessment_episode_matcher.importers.nada_indexed as io
from assessment_episode_matcher.mytypes import AODWarning, CSVTypeObject

def generate_nada_export(matched_assessments:pd.DataFrame, config:dict) \
          -> tuple[pd.DataFrame,list[AODWarning]]:
    res, warnings_aod = prep_dataframe_nada(matched_assessments, config)

    st = nada_df_generator.generate_finaloutput_df(res)        
    return st, warnings_aod


#  = "atom-matching"
def save_nada_data(data:pd.DataFrame, container:str, outfile:str):
  exp = AzureBlobExporter(container_name=container) #
  exp.export_dataframe(data_name=outfile, data=data) 


def get_matched_assessments(container_name, st_dt, end_dt):
  p_str = f"{st_dt}-{end_dt}"
  file_source = BlobFileSource(container_name
                                ,folder_path=f"{p_str}")
  df , fname = io.import_data(st_dt, end_dt
                              , file_source
                              , prefix="forstxt_"
                             , suffix="_matched")
  return df, fname


def generate_nada_save(reporting_start_str:str
        , reporting_end_str :str
        , container_name:str, config:dict) -> tuple[int, list[AODWarning]|None]:
  
  p_str = f"{reporting_start_str}-{reporting_end_str}"

  df_reindexed, fname  = get_matched_assessments(container_name
                                                 , reporting_start_str
                                                 , reporting_end_str)
  if not utdf.has_data(df_reindexed):
    msg = f"No Indexed NADA data file with name {fname} could be found"
    logging.error(msg)
    return 0, None
  
  nada, warnings_aod = generate_nada_export(df_reindexed, config)

  outfile = f"{p_str}/surveytxt_{p_str}.csv"
  save_nada_data(nada, container=container_name, outfile=outfile)

  msg = f"saved {len(nada)} NADA COMS records to {outfile}"
  logging.info(msg)
  return len(nada), warnings_aod


def write_aod_warnings(data:list[AODWarning]
                       , container_name:str, period_str:str) -> str:

  outfile = f"{period_str}/errors_warnings/aod_{period_str}_warn.csv"
  logging.info("Going to write AOD Warnings")
  
  header = ["SLK","RowKey","drug_name","field_name", "field_value"]
  warnings_list = CSVTypeObject(header=header, rows=data)
  exp = AzureBlobExporter(container_name=container_name)
  exp.export_csv(data_name=outfile, data=warnings_list)  
  return outfile


def get_essentials(container_name:str|None, qry_params:dict) -> tuple[dict,dict]:
  if not (qry_params and qry_params.get('s') and qry_params.get('e')):
    msg = f"unable to proceed without start and end dates."
    logging.exception(msg)
    return {}, {"error": msg }
   
  if not container_name:
      msg = f"unable to proceed without app config {ConfigKeys.AZURE_BLOB_CONTAINER.value}"
      logging.exception(msg)
      return {}, {"error": msg }
  
  config = load_blob_config(container_name)
  if not config:
      msg = f"unable to proceed without configuration.json file (blob-container: {container_name})."
      logging.exception(msg)
      return {}, {"error": msg }
  
  return config, {}


def run(start_yyyymmd:str, end_yyyymmd:str) -> dict:

  container_name = os.environ.get('AZURE_BLOB_CONTAINER',"")

  config, errors = get_essentials(container_name
                                  , qry_params={'s':start_yyyymmd
                                                , 'e':end_yyyymmd})
  if errors:
    return errors
  
  len_nada , warnings_aod = generate_nada_save(start_yyyymmd
                                               , end_yyyymmd
                                               , container_name
                                               , config)
  result = {"num_nada_rows": len_nada}
  logging.info(f"Recorded {len_nada} NADA records in storage.")

  if warnings_aod:    
    p_str = f"{start_yyyymmd}-{end_yyyymmd}"
    outfile = write_aod_warnings(warnings_aod, container_name, period_str=p_str)
    
    logging.info(f"Wrote AOD Warnings to {outfile}")
    result["warnings_len"] = len(warnings_aod)
  return result

