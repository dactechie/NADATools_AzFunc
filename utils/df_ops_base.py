
from datetime import datetime
import pandas as pd


def to_num_yn_none(x) -> str|None:
    if x == 'No':
        return '0'
    elif not pd.isna(x):
        return '1'
    else:
        return None

def to_num_bool_none(x:bool|None) -> str|None:
  if pd.isna(x):
      return None
  if x ==  True:
      return '1'
  return '0'
    
  
def transform_multiple(df1:pd.DataFrame, fields:list[str], transformer_fn)-> pd.DataFrame:
  df = df1.copy()  
  df[fields] = df[fields].apply(lambda field_series: field_series.apply(transformer_fn))
  return df

"""
  used to parse dates from Communicare extract of episodes
"""
def float_date_parser(date_val):
     # Check if the input is NaN (Not a Number)
    if pd.isna(date_val):
        return datetime.now().date()  # 
    
    # if not isinstance(date_str, int) :
    #     return datetime.now().date()  # Replace with today's date
    # Check if the date_str is not a string or is an empty string
    date_val = str(int(date_val)).zfill(8)
    
    # # Check if the string length is less than 8 characters
    # if len(date_str) < 8:
    #     # Possibly handle or log this case, as it's an unexpected format
    #     return None  # or choose an appropriate default value

    try:
        return pd.to_datetime(date_val, format='%d%m%Y').date()
    except ValueError:
        # Handle the case where the date_str is not a valid date
        # You can log this error if needed
        return None  # or choose an appropriate default value
    

def drop_fields(df:pd.DataFrame, fieldnames:list or str or tuple):
  df2 = df.drop(fieldnames, axis=1)
  return df2


def concat_drop_parent(df, df2 ,drop_parent_name:str) -> pd.DataFrame:
   return pd.concat([df.drop(drop_parent_name, axis=1), df2], axis=1)

def get_non_empty_list_items(df:pd.DataFrame, field_name:str) -> pd.DataFrame:
  # get only rows where the list is not empty
  df2 = df[ df[field_name].apply(lambda x: isinstance(x,list) and len(x) > 0  )]
  return df2


#df.loc[:,~df.columns.str.contains('num')]
def drop_notes_by_regex(df):
  # 'OtherAddictiveBehaviours.Other (detail in notes below)'
  df2 = df.loc[:,~df.columns.str.contains('Comment|Note|ITSP', case=False)]

  # df2 = df.loc[:,~df.columns.str.contains('Comment', regex=False)  # & ~df.columns.str.contains('Note', regex=False) 
  #            ]
  return df2


# get PDC - it is the first/only list item in the PDC list
def normalize_first_element (l1:pd.DataFrame, dict_key:str):#, support:Optional[dict]):
  
  masked_rows=  l1[(dict_key in l1) and l1[dict_key].apply(lambda x: isinstance(x,list) and len(x) > 0  )]
  
  # first dict of the list of dicts
  pdcs_df = masked_rows[dict_key].map(lambda x: x[0])
  normd_pdc:pd.DataFrame = pd.json_normalize(pdcs_df.to_list())  # index lost
  
  # l1.loc[7537,'PDC'] == masked_rows['PDC'][7537] == normd_pdc.loc[7317,:]
  l2 = masked_rows.reset_index(drop=True)
  result = concat_drop_parent(l2, normd_pdc, dict_key)
  return result


def get_right_only(matched_atoms: pd.DataFrame, atoms_df: pd.DataFrame, join_cols: list) -> pd.DataFrame:
    # Perform an outer join
    outer_merged_df = pd.merge(matched_atoms, atoms_df, how='outer',
                               left_on=join_cols, right_on=join_cols, indicator=True)
    # Filter rows that are only in atoms_df
    only_in_atoms_df = outer_merged_df[outer_merged_df['_merge']
                                       == 'right_only']
    # Drop the indicator column and keep only columns from atoms_df
    only_in_atoms_df = only_in_atoms_df.drop(columns=['_merge'])
    cleaned_df = only_in_atoms_df.dropna(axis=1, how='all')
    return cleaned_df


"""
  Mutually unmatched
  merge_cols = ['SLK', 'Program']
"""
def get_lr_mux_unmatched(left_df:pd.DataFrame, right_df:pd.DataFrame, merge_cols:list['str']) \
  -> tuple[pd.DataFrame, pd.DataFrame,pd.DataFrame, pd.DataFrame]:

  merged_df = pd.merge(left_df, right_df, on=merge_cols, how='outer', indicator=True)
  # Get non-matching rows for df1
  left_non_matching = merged_df[merged_df['_merge'] == 'left_only']

  # Get non-matching rows for df2
  right_non_matching = merged_df[merged_df['_merge'] == 'right_only']
  # Left outer join and filter for non-matching records
  # left_non_matching = pd.merge(left_df, right_df, how='left', left_on=merge_cols, right_on=merge_cols, indicator=True)
  # left_non_matching = left_non_matching[left_non_matching['_merge'] == 'left_only']

  # Right outer join and filter for non-matching records
  # right_non_matching = pd.merge(left_df, right_df, how='right', left_on=merge_cols, right_on=merge_cols, indicator=True)
  # right_non_matching = right_non_matching[right_non_matching['_merge'] == 'right_only']

  # Optionally, you can drop the '_merge' column if it's no longer needed
  left_non_matching.drop(columns=['_merge'], inplace=True)
  right_non_matching.drop(columns=['_merge'], inplace=True)

  # rows with common SLK, PRogram (good rows)
  common_rows = pd.merge(left_df, right_df, on=merge_cols, how='inner')
    
  # Step 2: Filter the original DataFrames to keep only the common rows
  common_left = left_df[left_df[merge_cols].isin(common_rows[merge_cols]).all(axis=1)]
  common_right = right_df[right_df[merge_cols].isin(common_rows[merge_cols]).all(axis=1)]

  return left_non_matching, right_non_matching, common_left, common_right

# """
#   get_lr_mux
#   LR - left and right join , mutually exclusive
# """
# def get_lr_mux(matched_atoms: pd.DataFrame, atoms_df: pd.DataFrame, join_cols: list) -> pd.DataFrame:
#     # Perform an outer join
#     outer_merged_df = pd.merge(matched_atoms, atoms_df, how='outer',
#                                left_on=join_cols, right_on=join_cols, indicator=True)
#     # Filter rows that are only in atoms_df
#     only_in_atoms_df = outer_merged_df[outer_merged_df['_merge']
#                                        == 'right_only']
#     # Drop the indicator column and keep only columns from atoms_df
#     only_in_atoms_df = only_in_atoms_df.drop(columns=['_merge'])
#     cleaned_df = only_in_atoms_df.dropna(axis=1, how='all')
#     return cleaned_df