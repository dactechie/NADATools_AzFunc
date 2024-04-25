
import datetime
import pandas as pd

def safe_convert_to_int_strs(df1:pd.DataFrame, float_columns):
  df2 = df1.copy()
  df = df1.copy()

  df2[float_columns] = df[float_columns].astype(str).replace('nan', '')
  # df2[float_columns] = df2[float_columns].replace('nan', '')

  for col in float_columns:
    mask = ~df[col].isna()
    df2.loc[mask, col] = df.loc[mask, col].astype(int).astype(str)
    
  return df2


def has_data(df:pd.DataFrame|None) -> bool:
   return not(df is None or df.empty)

def get_dupes_by_key(df:pd.DataFrame, key:str):
  
  counts = df.groupby(key)[key].value_counts()
  if counts.empty:
     return None

  duplicates = counts[counts > 1].index.tolist()
  if not duplicates:
    return None
  return df[ df[key].isin(duplicates)]


def get_last_day_n_months_ago(n_months_ago) -> datetime.date:
    current_date = datetime.date.today()
    first_day_of_current_month = datetime.date(current_date.year
                                               , current_date.month
                                               , n_months_ago)
    last_day_of_previous_month = first_day_of_current_month \
                                    - datetime.timedelta(days=1)
    last_end = last_day_of_previous_month
    return last_end

"""
  NOTE: if there are open episodes, it returns last_end as 
        the last date of the previous month
"""
def get_firststart_lastend(first_dt_series: pd.Series, last_dt_series: pd.Series):
    # Get the earliest date from the first datetime series
    first_start = first_dt_series.min()

    # Check if there are any non-null values in the last datetime series
    if last_dt_series.notnull().any():
        # Get the latest date from the last datetime series
        last_end = last_dt_series.max()
    else:
        # If all values in the last datetime series are null,
        # find the last day of the previous month
        last_end = get_last_day_n_months_ago(1)

    return first_start, last_end

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
  # "None of [Index(['Past4WkBeenArrested', 'Past4WkHaveYouViolenceAbusive'], dtype='object')] are in the [columns]"
  fields_indf = [f for f in fields if f in df.columns]
  if not fields_indf:
     return df
  # if len(fields_indf) !=  len(df.columns):
  #   fields_noindf = [f for f in fields if f not in df.columns]
  #   logging.error(f"fields {fields_noindf} not in df columns {df.columns}")

  df[fields_indf] = df[fields_indf].apply(lambda field_series: field_series.apply(transformer_fn))
  return df



def drop_fields(df:pd.DataFrame, fieldnames:list | str | tuple):
  to_remove = [col for col in fieldnames if col in df.columns ]
  df2 = df.drop(to_remove, axis=1)
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



def merge_keys(df1:pd.DataFrame, merge_fields:list[str], separator:str='_')\
              -> tuple[pd.DataFrame, str]:
  """
  Merges multiple columns from a DataFrame into a single column using '_'.

  Args:
      df (pandas.DataFrame): The DataFrame containing the columns to merge.
      cols (list): A list of column names to be merged.

  Returns:
      pandas.DataFrame: The original DataFrame with a new column containing the merged data.
  """  
  df = df1.copy()
  new_field = separator.join(merge_fields)
  merged_col = df[merge_fields].apply(lambda x: separator.join(x.astype(str)), axis=1)
  df[new_field] = merged_col  
  # df[f'{field1}_{field2}'] =  df[field1] + '_' + df[field2]
  return df, new_field


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