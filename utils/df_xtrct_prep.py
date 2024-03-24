
#from typing import TYPE_CHECKING
#if TYPE_CHECKING:
# import os
import pandas as pd
from datetime import datetime
from utils.dtypes import date_to_str

# import mylogger
import logging

from utils.io import get_data, read_parquet, write_parquet, read_csv_to_dataframe
from data_prep import limit_clients_active_inperiod #, prep_dataframe_episodes
from data_config import  ATOM_DB_filters 
from utils.df_ops_base import has_data, safe_convert_to_int_strs
from utils.dtypes import convert_to_datetime
from models.categories import Purpose
from configs import episodes as EpCfg

from data_config import EstablishmentID_Program,   notanswered_defaults

def filter_by_purpose(df:pd.DataFrame, filters:dict|None) -> pd.DataFrame:
  if not filters:
     return df
  return df[df ['Program'].isin(filters['Program'])]

# logger = mylogger.get(__name__)

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
  xtr_start_str = str(date_to_str(extract_start_date, str_fmt='yyyymmdd'))
  xtr_end_str = str(date_to_str(extract_end_date, str_fmt='yyyymmdd'))
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


def set_not_answered(df1:pd.DataFrame, notanswered_cols:list) -> pd.DataFrame:
  df = df1.copy()
  for col in notanswered_cols:
    df[col].replace('', -1, inplace=True)

  return df


def cols_prep(source_df, dest_columns, fill_new_cols) -> pd.DataFrame:
  df_final = source_df.reindex(columns=dest_columns, fill_value=fill_new_cols)
  
  # 'StandardDrinksPerDay' (_PerOccassionUse) -> Range/average calculation resutls in float
  float_cols = list(df_final.select_dtypes(include=['float']).columns )
                    #+ \
                    #[c for c in df_final.columns if   '_PerOccassionUse' in c]
  df_final = safe_convert_to_int_strs (df_final, float_cols)#.astype('Int64')

  df_final = set_not_answered(df_final, notanswered_cols=notanswered_defaults)
  return df_final

# List of column names in the CSV
column_names = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION', 'PMSEpisodeID', 'PMSPersonID', 'DOB', 'DOB STATUS', 'SEX', 'COUNTRY OF BIRTH', 'INDIGENOUS STATUS', 'PREFERRED LANGUAGE', 'SOURCE OF INCOME', 'LIVING ARRANGEMENT', 'USUAL ACCOMMODATION', 'CLIENT TYPE', 'PRINCIPAL DRUG OF CONCERN', 'PDCSubstanceOfConcern', 'ILLICIT USE', 'METHOD OF USE PRINCIPAL DRUG', 'INJECTING DRUG USE', 'SETTING', 'CommencementDate', 'POSTCODE', 'SOURCE OF REFERRAL', 'MAIN SERVICE', 'EndDate', 'END REASON', 'REFERRAL TO ANOTHER SERVICE', 'FAMILY NAME', 'GIVEN NAME', 'MIDDLE NAME', 'TITLE', 'SLK', 'MEDICARE NUMBER', 'PROPERTY NAME', 'UNIT FLAT NUMBER', 'STREET NUMBER', 'STREET NAME', 'SUBURB']
# >DATS_NSW All MonthlyForAutomation
# ESTABLISHMENT_IDENTIFIER
#            , GEOGRAPHICAL_LOCATION
#            , EPISODE_ID
#            , PERSON_ID
#            , DOB
#            , DOB_STATUS
#            , SEX
#            , COUNTRY_OF_BIRTH
#            , INDIGENOUS_STATUS
#            , PREFERRED_LANGUAGE
#            , SOURCE_OF_INCOME
#            , LIVING_ARRANGEMENT
#            , USUAL_ACCOMMODATION
#            , CLIENT_TYPE
#            , PRINCIPAL_DRUG_OF_CONCERN
#            , SPECIFY_DRUG_OF_CONCERN
#            , ILLICIT_USE
#            , METHOD_OF_USE_PRINCIPAL_DRUG
#            , INJECTING_DRUG_USE
#            , SETTING
#            , START_DATE
#            , POSTCODE
#            , SOURCE_OF_REFERRAL
#            , MAIN_SERVICE
#            , END_DATE
#            , END_REASON
#            , REFERRAL_TO_ANOTHER_SERVICE
#            , SLK

# columns_of_interest = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION'
#                          , 'EPISODE ID','PERSON ID', 'SPECIFY DRUG OF CONCERN'
#                         #  , 'PRINCIPAL DRUG OF CONCERN'
#                          , 'START DATE', 'END DATE', 'SLK']
# rename_columns = {
#       'SPECIFY DRUG OF CONCERN': 'PDCSubstanceOfConcern',
#    #   'PRINCIPAL DRUG OF CONCERN': 'PDCCode',
#       'START DATE': 'CommencementDate', 'END DATE': 'EndDate',
#       'EPISODE ID': 'PMSEpisodeID', 'PERSON ID': 'PMSPersonID',    
#     }
# date_cols=['START DATE', 'END DATE']


def prep_episodes(ep_df1:pd.DataFrame) -> pd.DataFrame:

  ep_df = ep_df1[EpCfg.columns_of_interest].copy()
  ep_df['Program'] = ep_df['ESTABLISHMENT IDENTIFIER'].map(EstablishmentID_Program)
  
#  convert_to_datetime(atom_df['AssessmentDate'], format='%Y%m%d')


  ep_df[EpCfg.date_cols[0]] = convert_to_datetime(ep_df[EpCfg.date_cols[0]],  format='%d%m%Y')
  ep_df[EpCfg.date_cols[1]] = convert_to_datetime(ep_df[EpCfg.date_cols[1]],  format='%d%m%Y')
                   


  # ep_df[EpCfg.date_cols] = ep_df[EpCfg.date_cols] \
  #                           .apply(lambda x: x.apply(parse_date))
  ep_df.rename(columns=EpCfg.rename_columns
            , inplace=True)
  return ep_df

  

# def skip_rows(data:list[list[str]]):
#   a = False
#   i = 0 
#   if not data:
#      logging.error("data is empty")
#      return data
#   while not a:
#     arr = data[i]
#     print(arr)
#     if isinstance(arr, list) and  len(arr) == len(column_names):
#       a = True 
#     if i > 10:
#       msg = "expected to get data before the 10th row"
#       logging.error(msg)
#       raise Exception(msg)
#     i = i + 1

#   return data[i-1:]

# #Please use 'date_format' instead, or read your data in as 'object' dtype and then call 'to_datetime'.  
# def df_from_list(data, rename_columns
#                    , columns_of_interest:list[str]
#                    , date_cols:list[str]) -> pd.DataFrame:
  
#     # Splitting each string into a list of values
#     # split_data = [row.split(',') for row in data]

#     # Extracting the header (first row) and the data (rest of the rows)
#     data = skip_rows(data)
#     headers = data[0]
#     print(headers)
#     # print(headers.split(','))
#     data_rows = data[1:]
#     print(data_rows)
#     # print('dr splot', data_rows.split(','))
#     # Creating a DataFrame
#     df = pd.DataFrame(data_rows, columns=headers)
#     # c = [c.replace(' ','_') for c in columns_of_interest]
#     df = df[columns_of_interest]
#     df = df[df['EPISODE ID'].notna()] # the csv import can have a last row -like
#             #'---------------------------726359919940929805'

#     # dt_cols = [c.replace(' ','_') for c in date_cols]
#     for dtcol in date_cols:
#        df.loc[:,dtcol] = df[dtcol].apply(float_date_parser)
       
#     df.rename(columns=rename_columns, inplace=True)
  
#     # df['CommencementDate'] = pd.to_datetime(df['CommencementDate'], format='%d%m%Y')
#     # df['EndDate'] = pd.to_datetime(df['EndDate'], format='%d%m%Y')    
#     return df


# # def load_and_parse_episode_csvs(directory):
# #     # List to hold dataframes
# #     dfs = []
    
# #     # Loop over all files in the directory
# #     for filename in os.listdir(directory):
# #         # Check if the file is a CSV
# #         if not filename.endswith('.csv'):
# #             continue
# #         filepath = os.path.join(directory, filename)
# #         try:
# #           df = load_and_parse_csv(filepath)
# #         except ValueError as e:
# #             logger.error(f"Error parsing dates in file {filepath} with error {str(e)}")
# #             # logger.error("The problematic row is:")

            
# #             continue  # Skip this file and move to the next one

# #         # Append the dataframe to the list
# #         dfs.append(df)
    
# #     # Concatenate all dataframes in the list
# #     final_df = pd.concat(dfs, ignore_index=True)

# #     return final_df

# def load_and_parse_episode_csvs(directory, columns_of_interest):
#     # List to hold dataframes
#     dfs = []
    
#     # Loop over all files in the directory
#     for filename in os.listdir(directory):
#         # Check if the file is a CSV
#         if not filename.endswith('.csv'):
#           continue
#         filepath = os.path.join(directory, filename)
#         # Load the CSV
#         df = pd.read_csv(filepath, header=None, names=column_names)
#         # Select only the columns we care about
#         df = df[columns_of_interest]
#         # Try to convert CommencementDate and EndDate columns to datetime format
#         try:
#             df['CommencementDate'] = pd.to_datetime(df['CommencementDate'], format='%d%m%Y',errors='coerce')
#             df['EndDate'] = pd.to_datetime(df['EndDate'], format='%d%m%Y', errors='coerce')
#         except ValueError as e:
#             logging.error(f"Error parsing dates in file {filename} with error {str(e)}")
#             logging.error("The problematic row is:")
#             logging.error(df.iloc[-1])
#             continue  # Skip this file and move to the next one
#         # Append the dataframe to the list
#         dfs.append(df)
    
#     # Concatenate all dataframes in the list
#     final_df = pd.concat(dfs, ignore_index=True)

#     return final_df
