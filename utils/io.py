
import os
# import csv
from datetime import datetime
import logging
import pandas as pd
# import mylogging
from azutil.helper import get_results
# from filters import get_outfilename_for_filters

# logging = mylogging.get(__name__)

def read_csv_to_dataframe(csv_file_path) -> pd.DataFrame:
    df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
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


def write_results_to_files(all_results, results_filename):# filters:dict = {}):
  
  # filt_fname = get_outfilename_for_filters(filters)

  for results in all_results:
    # title_for_file = results['title'].replace(" ", "_")
    data = results['data']
    data.insert(0,'ChartingDomain', results['title'])
    
    # data = add_filter_columns(data, filters)

    # results_filepath = f"{results_folder}{fname}_{title_for_file}.csv"
    # results_filepath = f"{results_folder}.csv"
    write_df_to_csv(data, results_filename)# results_filepath, filters)
    


# def write_df_to_csv(df, file_path:str, filters:dict|None={}):
#   df['ResultsTimestamp'] = datetime.now().replace(microsecond=0)
  
#   if not filters or not any(filters.values()):    
#     df.to_csv(file_path, index=False)
#     return
  
#   # add filter columns to DataFrame
#   for filter_name, filter_values in filters.items():
#     # join the list of filter values into a single string with semicolon
#     filter_values_str = '; '.join(filter_values)
#     df[filter_name] = filter_values_str

#   df.to_csv(file_path, index=False)

# def write_results_to_csv(results, file_path:str):
#   with open(file_path, 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile)
#     writer.writerow(results[0].keys())
#     for row in results:
#       writer.writerow(row.values())

def read_parquet(file_path:str) -> pd.DataFrame|None:
  if os.path.exists(file_path):
    df = pd.read_parquet(file_path)
    return df
  return None

def write_parquet(df:pd.DataFrame, file_path:str, force=False) -> pd.DataFrame|None:
  pathdirs_without_fname = file_path.split("/")[:-1]
  parent_dir = "/".join(pathdirs_without_fname)
  if force or os.path.exists(parent_dir):
    df.to_parquet(f"{file_path}")
    logging.info(f"Wrote to parquet file {file_path}")
    return
  logging.info(f"Did not write to  file {file_path}")
 

#TODO : store /load parquet file from file/blob storage
def get_data(table:str, start_date, end_date, download_filepath:str, filters:dict|None={}, cache=False) -> pd.DataFrame|None:
  #
  # get from ATOM Azure DB and save to disk
  #
  if cache:
    if os.path.exists(f"{download_filepath}"):
      logging.info(f"Using cached data from {download_filepath}")
      df = read_parquet(f"{download_filepath}")
      return df
    else:
      logging.info("No cached data found, loading from DB")

  results = get_results(table, start_date, end_date, filters)
  if not results:
    logging.error("Zero results returned from get_results (backend)")
    return None
  
  df = pd.DataFrame.from_records(results)
  df.to_parquet(f"{download_filepath}")
  return df
