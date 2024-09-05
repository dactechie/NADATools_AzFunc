import os
import logging
import pandas as pd
from assessment_episode_matcher.configs import load_blob_config
from assessment_episode_matcher.utils.environment import ConfigKeys
from assessment_episode_matcher.exporters.main import AzureBlobExporter
from assessment_episode_matcher.data_prep import prep_nada_fields
from assessment_episode_matcher.exporters import NADAbase as nada_df_generator
from assessment_episode_matcher.importers.main import  BlobFileSource
import assessment_episode_matcher.utils.df_ops_base as utdf
import assessment_episode_matcher.importers.nada_indexed as io
from assessment_episode_matcher.mytypes import AODWarning, CSVTypeObject
from assessment_episode_matcher.mytypes import DataKeys as dk
from assessment_episode_matcher.utils.fromstr import get_date_from_str

def generate_nada_export(matched_assessments:pd.DataFrame, reporting_start_str:str
                                                 , reporting_end_str:str, config:dict) \
          -> tuple[pd.DataFrame,list[AODWarning]]:
    matched_assessments1 = matched_assessments.copy()
    
    matched_assessments1["Stage"] = nada_df_generator\
                              .get_stage_per_episode(matched_assessments1)
    # limit by date range AssessmentDate
    reporting_start, reporting_end = get_date_from_str (reporting_start_str,"%Y%m%d") \
                                  , get_date_from_str (reporting_end_str,"%Y%m%d")
    asmtdt_field = dk.assessment_date.value
    reporting_start_ts = pd.Timestamp(reporting_start)
    reporting_end_ts = pd.Timestamp(reporting_end)

    filtered_assessments = matched_assessments1[
        (pd.to_datetime(matched_assessments1[asmtdt_field]) >= reporting_start_ts) & 
        (pd.to_datetime(matched_assessments1[asmtdt_field]) <= reporting_end_ts)
    ]

    # atoms_active_inperiod =\
    #     utdf.in_period(matched_assessments1, asmtdt_field, asmtdt_field,
    #                      reporting_start, reporting_end)    
    
    res, warnings_aod = prep_nada_fields(filtered_assessments, config)

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
  
  nada, warnings_aod = generate_nada_export(df_reindexed, reporting_start_str
                                                 , reporting_end_str, config)

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
    logging.exception(msg, stack_info=True, exc_info=True)
    return {}, {"error": msg }
   
  if not container_name:
      msg = f"unable to proceed without app config {ConfigKeys.AZURE_BLOB_CONTAINER.value}"
      logging.exception(msg, stack_info=True, exc_info=True)
      return {}, {"error": msg }
  
  config = load_blob_config(container_name)
  if not config:
      msg = f"unable to proceed without configuration.json file (blob-container: {container_name})."
      logging.exception(msg, stack_info=True, exc_info=True)
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

