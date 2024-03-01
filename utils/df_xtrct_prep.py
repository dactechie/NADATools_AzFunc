
#from typing import TYPE_CHECKING
#if TYPE_CHECKING:
# import os
from utils.dtypes import date_to_str
# from datetime import date
from typing import Literal
# import mylogger
import logging
from utils.io import get_data, read_parquet, write_parquet, read_csv_to_dataframe
from data_prep import prep_dataframe_nada \
      , limit_clients_active_inperiod #, prep_dataframe_episodes
from data_config import  ATOM_DB_filters 
from utils.df_ops_base import float_date_parser

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
def extract_prep_atom_data(extract_start_date, extract_end_date                              
                      , purpose:Literal['NADA', 'Matching']='Matching') :#-> pd.DataFrame|None:
  warnings = None
  xtr_start_str = date_to_str(extract_start_date, str_fmt='yyyymmdd')
  xtr_end_str = date_to_str(extract_end_date, str_fmt='yyyymmdd')
  period_range = f"{xtr_start_str}-{xtr_end_str}"
  processed_filepath = f"./data/processed/atom_{purpose}_{period_range}.parquet"
  
  logging.info(f"Attempting to load processed data from {processed_filepath}")

  processed_df = read_parquet(processed_filepath)
  
  if not(isinstance(processed_df, type(None)) or processed_df.empty):
    logging.debug("found & returning pre-processed parquet file.")
    return processed_df, None
  
  logging.info("No processed data found, loading from raw data.")
  
  filters = ATOM_DB_filters[purpose]
  raw_df = get_data('ATOM'
                    ,int(xtr_start_str), int(xtr_end_str)
                    , f"./data/in/atom_{purpose}_{period_range}.parquet"
                    ,filters=filters
                    , cache=True)
  
  if isinstance(raw_df, type(None)) or raw_df.empty:
    logging.error("No data found. Exiting.")
    return None, None
  
  # Clean and Transform the dataset
  if purpose == 'NADA':
    processed_df, warnings = prep_dataframe_nada(raw_df)
  else:
     raise NotImplementedError("Matching prep has not yet been implemented")
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
  return processed_df, warnings


import os
import pandas as pd


def cols_prep(source_df, dest_columns, fill_new_cols) -> pd.DataFrame:
  df_final = source_df.reindex(columns=dest_columns, fill_value=fill_new_cols)

  float_cols = df_final.select_dtypes(include=['float']).columns
  df_final[float_cols] = df_final[float_cols].astype('Int64')
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


#Please use 'date_format' instead, or read your data in as 'object' dtype and then call 'to_datetime'.  
def df_from_list(data, rename_columns
                   , columns_of_interest:list[str]
                   , date_cols:list[str]) -> pd.DataFrame:
  
    # Splitting each string into a list of values
    # split_data = [row.split(',') for row in data]

    # Extracting the header (first row) and the data (rest of the rows)
    headers = data[0]
    data_rows = data[1:]

    # Creating a DataFrame
    df = pd.DataFrame(data_rows, columns=headers)
    # c = [c.replace(' ','_') for c in columns_of_interest]
    df = df[columns_of_interest]

    # dt_cols = [c.replace(' ','_') for c in date_cols]
    for dtcol in date_cols:
       df.loc[:,dtcol] = df[dtcol].apply(float_date_parser)
       
    df.rename(columns=rename_columns, inplace=True)
  
    # df['CommencementDate'] = pd.to_datetime(df['CommencementDate'], format='%d%m%Y')
    # df['EndDate'] = pd.to_datetime(df['EndDate'], format='%d%m%Y')    
    return df


# def load_and_parse_episode_csvs(directory):
#     # List to hold dataframes
#     dfs = []
    
#     # Loop over all files in the directory
#     for filename in os.listdir(directory):
#         # Check if the file is a CSV
#         if not filename.endswith('.csv'):
#             continue
#         filepath = os.path.join(directory, filename)
#         try:
#           df = load_and_parse_csv(filepath)
#         except ValueError as e:
#             logger.error(f"Error parsing dates in file {filepath} with error {str(e)}")
#             # logger.error("The problematic row is:")

            
#             continue  # Skip this file and move to the next one

#         # Append the dataframe to the list
#         dfs.append(df)
    
#     # Concatenate all dataframes in the list
#     final_df = pd.concat(dfs, ignore_index=True)

#     return final_df

def load_and_parse_episode_csvs(directory, columns_of_interest):
    # List to hold dataframes
    dfs = []
    
    # Loop over all files in the directory
    for filename in os.listdir(directory):
        # Check if the file is a CSV
        if not filename.endswith('.csv'):
          continue
        filepath = os.path.join(directory, filename)
        # Load the CSV
        df = pd.read_csv(filepath, header=None, names=column_names)
        # Select only the columns we care about
        df = df[columns_of_interest]
        # Try to convert CommencementDate and EndDate columns to datetime format
        try:
            df['CommencementDate'] = pd.to_datetime(df['CommencementDate'], format='%d%m%Y',errors='coerce')
            df['EndDate'] = pd.to_datetime(df['EndDate'], format='%d%m%Y', errors='coerce')
        except ValueError as e:
            logging.error(f"Error parsing dates in file {filename} with error {str(e)}")
            logging.error("The problematic row is:")
            logging.error(df.iloc[-1])
            continue  # Skip this file and move to the next one
        # Append the dataframe to the list
        dfs.append(df)
    
    # Concatenate all dataframes in the list
    final_df = pd.concat(dfs, ignore_index=True)

    return final_df
