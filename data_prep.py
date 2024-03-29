import logging
# from typing import Literal
import pandas as pd

from data_config import keep_parent_fields, mulselect_option_to_nadafield
from models.categories import Purpose
from utils.base import check_for_string
from utils.dtypes import convert_dtypes, fix_numerics
from utils.df_ops_base import concat_drop_parent, \
                            drop_notes_by_regex \
                      ,   drop_fields,\
                          to_num_yn_none, to_num_bool_none,transform_multiple
from utils.fromstr import clean_and_parse_json
from data_config import EstablishmentID_Program, nada_cols #, activities_w_days
from process_odc_cols import expand_drug_info

# logger = mylogger.get(__name__)


def limit_clients_active_inperiod(df, start_date, end_date):
  # clients_inperiod = df[ (df.AssessmentDate >=  '2022-07-01') & (df.AssessmentDate <= '2023-06-30')].SLK.unique()
  logging.debug(f"Total clients {len(df)}")
  
  clients_inperiod = df[ (df.AssessmentDate >=  start_date) & (df.AssessmentDate <= end_date)].SLK.unique()
  df_active_clients = df [ df['SLK'].isin(clients_inperiod) ]
  
  logging.debug(f"Clients in period {len(df_active_clients)}")

  return df_active_clients

# TODO : creates blank rows: df_final[df_final.Program.isna()]
def get_surveydata_expanded(df:pd.DataFrame, prep_type:Purpose):#, prep_type:Literal['ATOM', 'NADA', 'Matching'] ) -> pd.DataFrame: 
  # https://dschoenleber.github.io/pandas-json-performance/
  
  logging.debug("\t get_surveydata_expanded")

  df_surveydata = df['SurveyData'].apply(clean_and_parse_json)
  df_surveydata_expanded:pd.DataFrame =  pd.json_normalize(df_surveydata.tolist(), max_level=1)
  if prep_type == Purpose.MATCHING:
    df_surveydata_expanded = df_surveydata_expanded[['ClientType', 'PDC']] 
  
  if keep_parent_fields:
    existing_columns_to_remove = [col for col in keep_parent_fields 
                                  if col in df_surveydata_expanded.columns]
    if existing_columns_to_remove:
      df_surveydata_expanded = drop_fields(df_surveydata_expanded, keep_parent_fields)
  # df_surveydata_expanded = df_surveydata_expanded[ keep_parent_fields[prep_type] ]
  df_final  = concat_drop_parent(df, df_surveydata_expanded, drop_parent_name='SurveyData')
  return df_final


# def ep_dates(raw_df:pd.DataFrame, columns:list[str])->pd.DataFrame:
#   df = raw_df.copy()
#   for col in columns:
#       if not pd.api.types.is_integer_dtype(df[col].dtype):
#         if (df[col] == '').any():
#           df[col] = df[col].replace('', 0).astype(int)
#         else:
#           df[col] = df[col].fillna(0).astype(int)

#       # Convert integer to string with zero-padding to ensure length is 8
#       df[col] = df[col].astype(str).str.zfill(8)
      
#       # Reformat string to match datetime format 'ddmmyyyy' -> 'dd-mm-yyyy'
#       df[col] = df[col].apply(lambda x: f"{x[:2]}-{x[2:4]}-{x[4:]}")
      
#       # Convert string to datetime
#       df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
#   return df  

def nadafield_from_multiselect(df1:pd.DataFrame) -> pd.DataFrame:
  df= df1.copy()
  # no_answer_value = -1  # do this together later for all fields.
  for ATOMMultiSelectQuestion, nadafield_searchstr in mulselect_option_to_nadafield.items():
    for nadafield, search_str in nadafield_searchstr.items():
      if ATOMMultiSelectQuestion not in df.columns:
        logging.warn(f"No column {ATOMMultiSelectQuestion} nadafield_from_multiselect")
        continue
      df[nadafield] = df[ATOMMultiSelectQuestion].apply(lambda x: check_for_string(x, search_str))

  return df


def convert_yes_nofields(df1, field_names):
  return transform_multiple(df1, field_names,to_num_yn_none)

def convert_true_falsefields(df1, field_names):
  return transform_multiple(df1, field_names,to_num_bool_none)


def prep_dataframe_matching(df:pd.DataFrame):

  logging.debug(f"prep_dataframe of length {len(df)} : ")
  df2 = get_surveydata_expanded(df.copy(),  Purpose.MATCHING)

  df5, warnings_aod = expand_drug_info(df2)
  return df5, warnings_aod


def prep_dataframe_nada(df:pd.DataFrame):

  logging.debug(f"prep_dataframe of length {len(df)} : ")
  df2 = get_surveydata_expanded(df.copy(), Purpose.NADA)
 
  df4 = drop_notes_by_regex(df2) # remove *Goals notes, so do before PDC step (PDCGoals dropdown)

  df5, warnings_aod = expand_drug_info(df4)

  # df51 = expand_activities_info(df5)
  df51 = nadafield_from_multiselect(df5)
  # df6 = df5[df5.PDCSubstanceOrGambling.notna()]# removes rows without PDC
  
  yes_nofields = ['Past4WkBeenArrested', 'Past4WkHaveYouViolenceAbusive']

  df52 = convert_yes_nofields(df51, yes_nofields)
  bool_fields = ['ATOPHomeless',	'ATOPRiskEviction',	'PrimaryCaregiver_0-5',
                 	'PrimaryCaregiver_5-15',	'Past4Wk_ViolentToYou',]
  df6 = convert_true_falsefields(df52, bool_fields)
   
  df7 = fix_numerics(df6)  
  df7.rename(columns={'ESTABLISHMENT IDENTIFIER': 'AgencyCode'}, inplace=True)
  
  df9 = df7.sort_values(by=["SLK", "AssessmentDate"])
  
  logging.debug(f"Done Prepping df")
  return df9 , warnings_aod


# def prep_dataframe(df:pd.DataFrame, prep_type: Literal['ATOM', 'NADA', 'Matching'] = 'ATOM'):
#    # because Program is in SurveyData
  
#   if prep_type == 'Matching':
#     return prep_dataframe_matching(df)

#   logger.debug(f"prep_dataframe of length {len(df)} : ")
#   df2 = get_surveydata_expanded(df.copy())

#   df3 = drop_fields(df2,['ODC'])
#   df4 = drop_cols_contains_regex(df3, ATOM_DROP_COLCONTAINS_REGEX) # remove *Goals notes, so do before PDC step (PDCGoals dropdown)
#   df5 = normalize_first_element(df4,'PDC') #TODO: (df,'ODC') # only takes the first ODC   

 
#   df6 = df5[df5.PDCSubstanceOrGambling.notna()]# removes rows without PDC

#   # df6.loc[:,'Program'] = df6['RowKey'].str.split('_').str[0] # has to be made into category
#   df7 = convert_dtypes(df6)

#   # df.PDCAgeFirstUsed[(df.PDCAgeFirstUsed.notna()) & (df.PDCAgeFirstUsed != '')].astype(int)
#  # "Expected bytes, got a 'int' object", 'Conversion failed for column PDCAgeFirstUsed with type object'
#   df8 = drop_fields(df7, ['PDCAgeFirstUsed',\
#                            'PrimaryCaregiver','Past4WkAodRisks']) 
#   # 'cannot mix list and non-list, non-null values', 
#   # 'Conversion failed for column PrimaryCaregiver, Past4WkAodRisks with type object')

#   if 'SLK' in df8.columns:
#     df8.drop(columns=['SLK'], inplace=True) 
  
#   df8.rename(columns={'PartitionKey': 'SLK'}, inplace=True)
  
#   df9 = df8.sort_values(by="AssessmentDate")
 
#   df9['PDC'] = df9['PDCSubstanceOrGambling']
 
#   logger.debug(f"Done Prepping df")
#   return df9