# from typing import Optional
import pandas as pd
# from datetime import datetime


# import mylogger
import logging

from utils.io import get_data, read_parquet, write_parquet, read_csv_to_dataframe
# from data_prep import limit_clients_active_inperiod #, prep_dataframe_episodes
from data_config import  ATOM_DB_filters 
from utils.df_ops_base import has_data, safe_convert_to_int_strs
from utils.dtypes import convert_to_datetime
from utils.base import get_period_range
from models.categories import Purpose, DataType


def filter_by_purpose(df:pd.DataFrame, filters:dict|None) -> pd.DataFrame:
  if not filters:
     return df
  return df[df ['Program'].isin(filters['Program'])]



def prepare(asmt_df:pd.DataFrame, start_date:str, end_date:str) -> pd.DataFrame:
  processed_folder = 'data/processed/'
  atom_df = asmt_df.rename(columns={'PartitionKey': 'SLK'})
  atom_df['AssessmentDate'] = convert_to_datetime(atom_df['AssessmentDate'], format='%Y%m%d')
  write_parquet(atom_df, f"{processed_folder}/ATOM_{start_date}-{end_date}-AllPrograms.parquet")
  return atom_df


def import_data(asmt_st:str, asmt_end:str) -> pd.DataFrame:
  processed_folder = 'data/processed/'
  source_folder = 'data/in/'

  period_range = f"{asmt_st}-{asmt_end}"
  fname =  f'ATOM_{period_range}-AllPrograms'
  processed_filepath = f"{processed_folder}/{fname}"
  
  logging.info(f"Attempting to load processed data from {processed_filepath}")

  processed_df = read_parquet(f"{processed_filepath}.parquet")
  
  if not(isinstance(processed_df, type(None)) or processed_df.empty):
    logging.debug("found & returning pre-processed parquet file.")
    # TODO chec if the timestamp on this the file is recent
    # get the last modified date of the file
    # get the last modified date of ATOMs in the period of interest (assessmentDate)
    # if the last modified date of the file is after the last modified date of ATOMs, then return the processed_df
    # else query Azure data to get the latest ATOMs and merge them into the processed_df and save to disk to override
    return processed_df
  
  logging.info("No processed data found, loading from raw data.")
  
  
  # cache data for all programs  - to parquet
  raw_df = get_data('ATOM'
                    ,int(asmt_st), int(asmt_end)
                    , f"{source_folder}/{fname}.parquet"
                    ,filters=None
                    , cache=True)
  
  if not has_data(raw_df):
     logging.info("Returning Empty Dataframe.")
     return pd.DataFrame()
  
  atom_df = prepare(raw_df, asmt_st, asmt_end)
  return atom_df
  


# def get_filename(data_type:DataType, purpose:Optional[Purpose]) -> str:
#   if data_type == DataType.ATOM:
#     return f"./data/raw/atom_{purpose}.csv"
  
#   filepath = f"./data/processed/atom_{purpose}_{period_range}.parquet"
#   return filepath


# if there is no pre-processed data, then extract and process it
# Processing it includes:
#   - dropping fields that are not needed
#   - limiting to clients who have at least 1 assessment in the period of interest
#   - limiting to clients who have completed at least 3 surveys
#   - converting data types
#   - normalizing data (if there are nested data structures like SurveyData)
#   - caching the processed version

# Note:  purpose =matching :  may be ACT also 
"""
  returns processed (cached) or un_processed data 
  if returning processed data, the 2nd param is True
"""
def extract_atom_data(extract_start_date, extract_end_date                              
                            , purpose:Purpose) -> tuple[pd.DataFrame, bool] :
  # warnings = None
  is_processed = False
  xtr_start_str, xtr_end_str = get_period_range(extract_start_date, extract_end_date)
  period_range = f"{xtr_start_str}-{xtr_end_str}"
  processed_filepath = f"./data/processed/atom_{purpose}_{period_range}.parquet"
  
  logging.info(f"Attempting to load processed data from {processed_filepath}")

  processed_df = read_parquet(processed_filepath)
  
  if not(isinstance(processed_df, type(None)) or processed_df.empty):
    logging.debug("found & returning pre-processed parquet file.")
    # TODO chec if the timestamp on this the file is recent
    # get the last modified date of the file
    # get the last modified date of ATOMs in the period of interest (assessmentDate)
    # if the last modified date of the file is after the last modified date of ATOMs, then return the processed_df
    # else query Azure data to get the latest ATOMs and merge them into the processed_df and save to disk to override
    return processed_df, True
  
  logging.info("No processed data found, loading from raw data.")
  
  
  # cache data for all programs
  raw_df = get_data('ATOM'
                    ,int(xtr_start_str), int(xtr_end_str)
                    , f"./data/in/atom_{period_range}.parquet"
                    ,filters=None
                    , cache=True)
  
  if not has_data(raw_df):
     return pd.DataFrame(), is_processed
     
 
  raw_df = filter_by_purpose(raw_df, ATOM_DB_filters[purpose])
  
  if isinstance(raw_df, type(None)) or raw_df.empty:
    logging.error("No data found. Exiting.")
    return pd.DataFrame(), is_processed
  
  raw_df['AssessmentDate'] = convert_to_datetime(raw_df['AssessmentDate'], format='%Y%m%d')
  
  return raw_df, is_processed

  # TODO: getting an error when caching processed results
    # processed_df = prep_dataframe(raw_df, prep_type=purpose) # only one filter: PDCSubstanceOrGambling has to have a value
    
  # if active_clients_start_date and active_clients_end_date:
  #   processed_df = limit_clients_active_inperiod(processed_df, active_clients_start_date, active_clients_end_date)
    
  # cache the processed data
  # processed_df.to_parquet(f"{processed_filepath}")
  # try:
  #   write_parquet(processed_df, processed_filepath) # don't force overwrite
  #   logger.info(f"Done saving processed data to {processed_filepath}")
  # except ArrowTypeError as re:
  #   logger.error(f"ArrowTypeError: {re}. unable to save parquet file.")     
  # except Exception as ae:
  #   logger.error(f"ArrowTypeError: {ae}. unable to save parquet file.")    
  # finally: