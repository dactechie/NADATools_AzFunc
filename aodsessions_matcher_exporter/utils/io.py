
import os
from datetime import datetime
import logging
import pandas as pd
from utils.dtypes import convert_to_datetime
import utils.df_ops_base as utdf
from azutil.helper import get_results
# from filters import get_outfilename_for_filters

# logging = mylogging.get(__name__)

def read_csv_to_df(csv_file_path, dtype) -> pd.DataFrame:
    if not os.path.exists(csv_file_path):
      return pd.DataFrame()
    if not dtype:
      df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
    else:
      df = pd.read_csv(csv_file_path, dtype=str,  encoding='utf-8-sig')

    df.fillna("", inplace=True)
    return df

# def read_csv_to_dict_list(csv_file_path):
#     with open(csv_file_path, mode='r', encoding='utf-8-sig') as file:
#         csv_reader = csv.DictReader(file)
#         data = [row for row in csv_reader]
#     return data

def create_results_folder(results_folder:str):
  # create results folder if it doesn't exist
  if not os.path.exists(results_folder):
    os.makedirs(results_folder)

def write_df_to_csv(df, file_path:str, filters:dict|None={}):
  df['ResultsTimestamp'] = datetime.now().replace(microsecond=0)
  # Check if file exists
  if os.path.isfile(file_path):
      # If the file exists, append without writing the header
      df.to_csv(file_path, mode='a', index=False, header=False)
  else:
      # If the file does not exist, write the DataFrame to CSV with a header
      df.to_csv(file_path, index=False, header=True)  

  # df.to_csv(file_path, index=False, mode='a')


def add_filter_columns(df1, filters:dict):
  if not filters or not any(filters.values()):
    return df1
  
  df = df1.copy()
  
  # add filter columns to DataFrame
  for filter_name, filter_values in filters.items():
    # join the list of filter values into a single string with semicolon
    filter_values_str = '; '.join(filter_values)
    df[filter_name] = filter_values_str

  return df
    

def read_parquet(file_path:str) -> pd.DataFrame:
  if os.path.exists(file_path):
    df = pd.read_parquet(file_path)
    return df
  return pd.DataFrame()

def write_parquet(df:pd.DataFrame, file_path:str, force=False) -> int:
  pathdirs_without_fname = file_path.split("/")[:-1]
  parent_dir = "/".join(pathdirs_without_fname)
  if force or os.path.exists(parent_dir):
    df.to_parquet(f"{file_path}")
    logging.info(f"Wrote to parquet file {file_path}")
    return 0
  logging.info(f"Did not write to  file {file_path}")
  return -1
 

def get_from_source(table:str, start_date:int, end_date:int
                  , filters:dict|None={}):#, add_to_cache:bool=False):
  results = get_results(table, start_date, end_date, filters)
  if not results:
    logging.info("Zero results returned from get_results (backend)")
    return pd.DataFrame()
  
  df = pd.DataFrame.from_records(results)
  
  return df


def get_lastmod_utcstr(timestamp:pd.Series) -> str:
  max_timestamp = max(timestamp)
  s = max_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
  return s



def process_assment(asmt_df:pd.DataFrame) -> pd.DataFrame:
  
  atom_df = asmt_df.rename(columns={'PartitionKey': 'SLK'})
  atom_df['AssessmentDate'] = convert_to_datetime(atom_df['AssessmentDate'], format='%Y%m%d'
                                                  , fill_blanks=False)
  
  return atom_df


def refresh_dataset(df1:pd.DataFrame, df2:pd.DataFrame):
  """
    It is assumed that df1 doesn't have the IsActive column 
    - it is redundant to store it locally (expect all rows to be active)

  """
  key_columns = ['SLK', 'RowKey']
  df1_len = len(df1)
  df2_len = len(df2)
  
  # remove deactivated rows from d1
  inactive_rows = df2[df2['IsActive'] == 0][key_columns]
  logging.info(f"Pre-merge #rows: {df1_len}. rows to be made inactive: {len(inactive_rows)}  (within df2) df2 len:{df2_len}.)")  
  df1 = df1[~df1.set_index(key_columns).index.isin(inactive_rows.set_index(key_columns).index)]

  # Remove the IsActive column from df2 (after keeping only active rows)
  df2 = df2[df2['IsActive'] == 1].drop(columns=['IsActive'])

  merged_updated = utdf.update(df1, df2, on=key_columns)
  merged_len = len(merged_updated)
  logging.info(f" Original len() df: {len(df1)} Merged df length {merged_len}.")
  
  # merged_updated = merged_updated[merged_updated['IsActive'] == 1]
  # logging.info(f"Removed {merged_len-len(merged_updated)} assessments from cache (IsActive=0)")
  return merged_updated  

#unproecessed
def handle_refresh(df1:pd.DataFrame
                   , table:str, start_date:int, end_date:int
             , filters) -> tuple[pd.DataFrame, bool]:
    
    if utdf.has_data(df1) and 'Timestamp' in df1.columns:
      latest_date = get_lastmod_utcstr(df1.Timestamp)
      filters['Timestamp'] = latest_date
    df2 =  get_from_source(table, start_date, end_date,filters)

    if not utdf.has_data(df2):
      return df1, False
    
    df2_processed = process_assment(df2)
    if not utdf.has_data(df1):
      return df2_processed, True
    
    merged_updated = refresh_dataset(df1, df2_processed)
    
    
    return merged_updated, True


#TODO : store /load parquet file from file/blob storage
def get_data(table:str, start_date:int, end_date:int
             , download_filepath:str
             , filters:dict|None={}
             , cache:bool=False
             , refresh:bool=True) -> tuple[pd.DataFrame, bool]:
  #
  # get from ATOM Azure DB and save to disk
  #
  result_df = pd.DataFrame()
  was_refreshed = False
  if cache and os.path.exists(f"{download_filepath}"):
      # if os.path.exists(f"{download_filepath}"):
      logging.info(f"Using cached data from {download_filepath}")
      result_df = read_parquet(f"{download_filepath}")

      if refresh:
        result_df, was_refreshed = handle_refresh(result_df
                                  , table
                                  , start_date
                                  , end_date
                                  , filters )
      return result_df, was_refreshed

  else:
    logging.info("No cached data found, loading from DB")

  result_df = get_from_source(table
                      ,start_date
                      , end_date                      
                      , filters)
  # result_df.to_parquet(f"{download_filepath}")

  return result_df, True


# if __name__ == '__main__':
#     df1 = pd.DataFrame({'PartitionKey': ['A', 'B', 'C', 'E'],
#                         'RowKey': [1, 2, 3, 4],
#                         'Value': [10, 20, 30, 15],
#                         'IsActive': [1,1,1,1,1]
                        
#                         })
#     df2 = pd.DataFrame({'PartitionKey': ['B', 'C', 'D', 'E'],
#                         'RowKey': [2, 3, 4, 4],
#                         'Value': [40, 50, 60, 15],
#                            'IsActive': [1,1,1,1,0]})
    
#     merged_df = refresh_dataset(df1, df2)
#     print(merged_df)


# def write_results_to_files(all_results, results_filename):# filters:dict = {}):
  
#   # filt_fname = get_outfilename_for_filters(filters)

#   for results in all_results:
#     # title_for_file = results['title'].replace(" ", "_")
#     data = results['data']
#     data.insert(0,'ChartingDomain', results['title'])
    
#     # data = add_filter_columns(data, filters)

#     # results_filepath = f"{results_folder}{fname}_{title_for_file}.csv"
#     # results_filepath = f"{results_folder}.csv"
#     write_df_to_csv(data, results_filename)# results_filepath, filters)