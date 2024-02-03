
''' 
  The Survey JSON has various types of data : text/string, dates, numeric, number-ranges.
  The data types are defined in data_config.py and this module uses that information to 
  convert the data to the correct type.
  
'''
import logging 
import pandas as pd
from pandas.api.types import CategoricalDtype
# import mylogging
from data_config import predef_categories,\
    question_list_for_categories, data_types, option_variants #, remove_if_under_threshold

# from utils.fromstr import range_average

# logging = mylogging.get(__name__)

    # - pandas.Categorical is used to create a variable that holds categorical data.

    # - pandas.api.types.CategoricalDtype is a type useful for specifying the categories and 
    # order when creating a pandas.Categorical variable, 
    # or when converting a pandas Series to categorical.
###############################################

def define_category_with_uniqs(df:pd.DataFrame, question:str) -> CategoricalDtype:  
  unique_list = list(df[df[question].notna()][question].unique())
  return CategoricalDtype(unique_list)
  

def define_category_for_question(df:pd.DataFrame, field:str):
  # if not a predefined category, define it using the unique values
  category_type = predef_categories.get(field,
                         define_category_with_uniqs(df, field) )
  return category_type


def define_all_categories(df:pd.DataFrame):
  category_nametypes = [(field
                          , define_category_for_question(df, field)
                        )
                        for field in question_list_for_categories]
  
  logging.debug(f"category_nametypes: {category_nametypes}")

  df1 = df.copy()  
  # Field elements must be 2- or 3-tuples, got ''Yes - Completely safe''
  for category_name, category_type in category_nametypes:
    #if category_type is not None and category_type..check type:
    # (isinstance(series.dtype, pd.api.types.CategoricalDtype):):
    df1[category_name] = df[category_name].astype(category_type)
  
  return df1
  
###############################################

def convert_to_datetime(df, column_names:list[str]|str):

  df [column_names] = pd.to_datetime(df[column_names], errors='coerce')

"""
      # fix_variants       
      # d1 = df.loc[df['PDCMethodOfUse'] =='Ingests'].copy()
      # d1.loc[:,'PDCMethodOfUse'] = 'Ingest'
      # df.update(d1)

"""
def fix_variants (df1):

  has_variant_column_types = [ov for ov in option_variants.keys() if ov in df1.columns ]
  if not any(has_variant_column_types):
    return df1
  
  df = df1.copy()
  for field, variant_dict in option_variants.items():  # type: ignore #PDCMethodOfUse
    for variant, original in variant_dict.items(): # type: ignore
      logging.info(f"fixing {field} {variant} to {original}")
      d1 = df.loc[df[field] == variant].copy()
      d1.loc[:,field] = original
      df.update(d1)
  return df


  # convert numeric types
def fix_numerics(df1):

  df = df1.copy()
  
  numeric_fields = [k for k, v in data_types.items() if v == 'numeric']
  logging.debug(f"numeric_fields: {numeric_fields}")
  df[numeric_fields] = df[numeric_fields].apply(pd.to_numeric, errors='coerce') # ignore ?

  # range_fields = [k for k, v in data_types.items() if v == 'range']
  # range_fields = [ f for f in df.columns
  #                    for sx in fieldname_suffixes_range
  #                       if f"_{sx}" in f ]
  # logging.debug(f"range_fields: {range_fields}") 
  # df[range_fields] = df[range_fields].applymap(range_average)

  return df


def convert_dtypes(df1):
  logging.debug(f"convert_dtypes")
  df = df1.copy()
  
  convert_to_datetime(df,'AssessmentDate') # TODO : DOB
  
  df1 = fix_variants(df) # Smokes -> Smoke   # NOT FOR NADA
  df2 = fix_numerics(df1)
  # df3 = define_all_categories(df2)  # not for NADA
  # return df3
  return df2
  