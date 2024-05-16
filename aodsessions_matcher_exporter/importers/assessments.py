import logging

import pandas as pd
from utils import io
from data_config import  ATOM_DB_filters 
from utils.df_ops_base import has_data

from mytypes import Purpose


def filter_by_purpose(df:pd.DataFrame, filters:dict|None) -> pd.DataFrame:
  if not filters:
     return df
  return df[df ['Program'].isin(filters['Program'])]




# def get_from_source(asmt_st:str, asmt_end:str, after_lastmod_date:Optional[str]="")

def fetch_and_process(table, start_dt:str, end_dt:str,
                      cachefile:str, filters, refresh=True):

  # cache data for all programs  - to parquet
  atom_df, was_refreshed = io.get_data(table
                    ,int(start_dt), int(end_dt)
                    , cachefile
                    ,filters=filters
                    , cache=True
                    , refresh=refresh)
  
  if not has_data(atom_df):
     logging.info("Returning Empty Dataframe.")
     return pd.DataFrame()
  
  # atom_df = process(raw_df)
  if was_refreshed:    
    io.write_parquet(atom_df, cachefile)# f"{processed_folder}/ATOM_{start_dt}-{end_dt}-AllPrograms.parquet")
  return atom_df


def import_data(asmt_st:str, asmt_end:str
                ,purpose:Purpose, refresh:bool=True
                ) -> pd.DataFrame:
  processed_folder = 'data/processed'
  # source_folder = 'data/in'
  filters = ATOM_DB_filters[purpose]
  
  period_range = f"{asmt_st}-{asmt_end}"
  fname =  f'ATOM_{period_range}-AllPrograms'
  processed_filepath = f"{processed_folder}/{fname}"

  
  # logging.info(f"Attempting to load processed data from {processed_filepath}")

  # processed_df = io.read_parquet(f"{processed_filepath}.parquet")
  
  # if not(isinstance(processed_df, type(None)) or processed_df.empty):
  #   logging.debug("found & returning pre-processed parquet file.")
    
  #   # get the last modified date of the file
  #   # get the last modified date of ATOMs in the period of interest (assessmentDate)
  #   # if the last modified date of the file is after the last modified date of ATOMs, then return the processed_df
  #   # else query Azure data to get the latest ATOMs and merge them into the processed_df and save to disk to override
    
  #   # TODO :

  #   if not refresh:
  #     return processed_df
  #     # logging.info("refreshed data")
  processed_df = fetch_and_process(table='ATOM'
                          ,start_dt=asmt_st
                          ,end_dt=asmt_end
                            , cachefile=f"{processed_filepath}.parquet"
                          , filters=filters
                          , refresh=refresh)  
        
  return processed_df
  


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


# """
#   returns processed (cached) or un_processed data 
#   if returning processed data, the 2nd param is True
# """
# def extract_atom_data(extract_start_date, extract_end_date                              
#                             , purpose:Purpose) -> tuple[pd.DataFrame, bool] :
#   # warnings = None
#   is_processed = False
#   xtr_start_str, xtr_end_str = get_period_range(extract_start_date, extract_end_date)
#   period_range = f"{xtr_start_str}-{xtr_end_str}"
#   processed_filepath = f"./data/processed/atom_{purpose}_{period_range}.parquet"
  
#   logging.info(f"Attempting to load processed data from {processed_filepath}")

#   processed_df = io.read_parquet(processed_filepath)
  
#   if not(isinstance(processed_df, type(None)) or processed_df.empty):
#     logging.debug("found & returning pre-processed parquet file.")
#     # TODO chec if the timestamp on this the file is recent
#     # get the last modified date of the file
#     # get the last modified date of ATOMs in the period of interest (assessmentDate)
#     # if the last modified date of the file is after the last modified date of ATOMs, then return the processed_df
#     # else query Azure data to get the latest ATOMs and merge them into the processed_df and save to disk to override
#     return processed_df, True
  
#   logging.info("No processed data found, loading from raw data.")
  
  
#   # cache data for all programs
#   raw_df  = io.get_data('ATOM'
#                     ,int(xtr_start_str), int(xtr_end_str)
#                     , f"./data/in/atom_{period_range}.parquet"
#                     ,filters=None
#                     , cache=True)
  
#   if not has_data(raw_df):
#      return pd.DataFrame(), is_processed
     
 
#   raw_df = filter_by_purpose(raw_df, ATOM_DB_filters[purpose])
  
#   if isinstance(raw_df, type(None)) or raw_df.empty:
#     logging.error("No data found. Exiting.")
#     return pd.DataFrame(), is_processed
  
#   raw_df['AssessmentDate'] = convert_to_datetime(raw_df['AssessmentDate'], format='%Y%m%d'
#                                                  , fill_blanks=False)
  
#   return raw_df, is_processed

#   # TODO: getting an error when caching processed results
#     # processed_df = prep_dataframe(raw_df, prep_type=purpose) # only one filter: PDCSubstanceOrGambling has to have a value
    
#   # if active_clients_start_date and active_clients_end_date:
#   #   processed_df = limit_clients_active_inperiod(processed_df, active_clients_start_date, active_clients_end_date)
    
#   # cache the processed data
#   # processed_df.to_parquet(f"{processed_filepath}")
#   # try:
#   #   write_parquet(processed_df, processed_filepath) # don't force overwrite
#   #   logger.info(f"Done saving processed data to {processed_filepath}")
#   # except ArrowTypeError as re:
#   #   logger.error(f"ArrowTypeError: {re}. unable to save parquet file.")     
#   # except Exception as ae:
#   #   logger.error(f"ArrowTypeError: {ae}. unable to save parquet file.")    
#   # finally: