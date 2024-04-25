
#from typing import TYPE_CHECKING
#if TYPE_CHECKING:
# import os
# import pandas as pd




# List of column names in the CSV
# column_names = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION', 'PMSEpisodeID', 'PMSPersonID', 'DOB', 'DOB STATUS', 'SEX', 'COUNTRY OF BIRTH', 'INDIGENOUS STATUS', 'PREFERRED LANGUAGE', 'SOURCE OF INCOME', 'LIVING ARRANGEMENT', 'USUAL ACCOMMODATION', 'CLIENT TYPE', 'PRINCIPAL DRUG OF CONCERN', 'PDCSubstanceOfConcern', 'ILLICIT USE', 'METHOD OF USE PRINCIPAL DRUG', 'INJECTING DRUG USE', 'SETTING', 'CommencementDate', 'POSTCODE', 'SOURCE OF REFERRAL', 'MAIN SERVICE', 'EndDate', 'END REASON', 'REFERRAL TO ANOTHER SERVICE', 'FAMILY NAME', 'GIVEN NAME', 'MIDDLE NAME', 'TITLE', 'SLK', 'MEDICARE NUMBER', 'PROPERTY NAME', 'UNIT FLAT NUMBER', 'STREET NUMBER', 'STREET NAME', 'SUBURB']
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
