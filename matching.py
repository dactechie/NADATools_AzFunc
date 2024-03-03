
import logging
import pandas as pd
from data_config import EstablishmentID_Program
from warnings_errors import  add_to_issue_report, IssueLevel,\
         IssueType, get_outofbounds_issues, get_duplicate_issues 

def prep_for_match(ep_df) -> pd.DataFrame:
    ep_df['Program'] = ep_df['ESTABLISHMENT IDENTIFIER'].map(EstablishmentID_Program)
    return ep_df


def get_stage_per_episode(df:pd.DataFrame)-> pd.Series:  
  df = df.sort_values(by=["PMSEpisodeID", "AssessmentDate"])
  # Rank the assessments within each client
  return  df.groupby('PMSEpisodeID').cumcount()


def get_mask_datefit(row, slack_days=7):
    # Create a Timedelta for slack days
    slack_td = pd.Timedelta(days=slack_days)

    after_commencement = row['AssessmentDate'] >= (row['CommencementDate'] - slack_td)
    before_end_date = row['AssessmentDate'] <= (row['EndDate'] + slack_td)
    return after_commencement and before_end_date


def match_dates_assessments_episodes(ep_atom_df, matching_ndays_slack: int):
    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    mask = ep_atom_df.apply(get_mask_datefit, slack_days=matching_ndays_slack, axis=1)

    filtered_df = ep_atom_df[mask]
    
    return filtered_df


def check_atom_in_multi_episodes(ep_atom_df:pd.DataFrame):

  counts = ep_atom_df.groupby('SLK_RowKey')['SLK_RowKey'].value_counts()
  if counts.empty:
     return None
  
  duplicates = counts[counts > 1].index.tolist()
  if not duplicates:
    return None
  return ep_atom_df[ ep_atom_df.SLK_RowKey.isin(duplicates)]



def not_in_source_df(slk_program_merged:pd.DataFrame
                     , source_df:pd.DataFrame
                    #  , vm:Type[ValidationMeta]
                     , issue_type:IssueType
                     ,  key:str):
  not_in_source = source_df[~source_df[key].isin(slk_program_merged[key])]
  errors_warnings = add_to_issue_report(not_in_source
                                # , vm
                                , issue_type
                                , issue_level=IssueLevel.ERROR)
  return errors_warnings

def match_by_matching_keys(ep_df, atom_df, matching_keys=['SLK', 'Program']):
  
  slk_program_merged = pd.merge(episodes_df, atoms_df, how='inner'
                                , left_on=matching_keys, right_on=matching_keys)
  errors_warnings_atom = not_in_source_df(slk_program_merged
                                          , atom_df
                                          , issue_type=IssueType.ONLY_IN_ASSESSMENT                           
                                          , key='SLK_RowKey')
  errors_warnings_ep = not_in_source_df(slk_program_merged
                                          , ep_df
                                          , issue_type=IssueType.ONLY_IN_EPISODE
                                          , key='SLK_Program')
    
  errors_warnings_atom.extend(errors_warnings_ep)
  return slk_program_merged, errors_warnings_atom


def match_increasing_slack(slk_program_matched:pd.DataFrame, max_slack:int=7):
  matching_ndays_slack = 0 
  unmatched_by_date = slk_program_matched
  result_matched_dfs = []
  result_matched_df = []
  duplicate_rows_dfs:list[pd.DataFrame] = []
  # atom_df['SLK_RowKey'] =  atom_df['SLK'] + '_' + atom_df['RowKey']
  # atom_df['SLK_RowKey'] =  atom_df['SLK'] + '_' + atom_df['RowKey']
  # ep_df['SLK_Program'] =  ep_df['SLK'] + '_' + ep_df['Program']
  

  # program_matched_slk_rks = slk_program_matched.SLK_RowKey

  while len(unmatched_by_date) > 0  and matching_ndays_slack <= max_slack:
      # Get matched assessments with the current slack
      matched_df = match_dates_assessments_episodes(unmatched_by_date, matching_ndays_slack)
      duplicate_rows_df = check_atom_in_multi_episodes(matched_df)
      if not (duplicate_rows_df is None or duplicate_rows_df.empty):      
        #logging.error("Duplicate rows", duplicate_rows_df)
        duplicate_rows_dfs.append(duplicate_rows_df)

      if len(matched_df) == 0: # no more SLK+Program matches between Episode and ATOM
         break
        # result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)
        # unmatched_by_date = unmatched_by_date[~unmatched_by_date.SLK_RowKey.isin(program_matched_slk_rks)]
        # return result_matched_df, unmatched_by_date, errors_warnings
      
      # Add the matched DataFrame to the list
      result_matched_dfs.append(matched_df)

      unmatched_by_date = unmatched_by_date[~unmatched_by_date.SLK_RowKey.isin(matched_df.SLK_RowKey)]

      ## there may be other assessments for this SLK that can match if the slack dways are increased
      ## don't exclude the SLK, but the SLK +RowKey

      # Increment the slack days for the next iteration
      matching_ndays_slack += 1

  if len(unmatched_by_date) > 0 :
     logging.info(f"There are still {len(unmatched_by_date)} unmatched ATOMs")
     logging.info(f"Unmatched by program: {len(unmatched_by_date.Program.value_counts())}")
    #  logger.info(f"There are still {len(unmatched_atoms)} unmatched ATOMs")
    #  logger.info(f"Unmatched by program: {len(unmatched_atoms.Program.value_counts())}")

  # Concatenate all matched DataFrames from the list
  if result_matched_dfs:
    result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)

  # add_to_issue_report(unmatched_by_date, IssueType.DATE_MISMATCH, IssueLevel.ERROR)
  return result_matched_df, unmatched_by_date, duplicate_rows_dfs


def merge_key(df1, field1, field2):
  df = df1.copy()
  df[f'{field1}_{field2}'] =  df[field1] + '_' + df[field2]
  return df

if __name__ == "__main__":
  # from datetime import datetime
  
  warning_limit_days = 3
  error_max_slack = 7
  match_types_testcases = [ 
    {
      'name':'atom_fits_multiple_eps', #: bad 
      'data':{
        'atoms_df':pd.DataFrame({'SLK':[ '1MidOfMyEp'],
              'RowKey':[ 'rk1'],'Program':['TSS'  ],
              'AssessmentDate':pd.to_datetime([ '2023-07-01' ])    
               }), 
        'episodes_df': pd.DataFrame({
            'SLK':['1MidOfMyEp', '1MidOfMyEp',  ],
            'Program':[ 'TSS', 'TSS'],
            'CommencementDate': pd.to_datetime([
                                                '2023-06-01', '2023-07-01'
                                                ]),
            'EndDate': [
                        pd.to_datetime('2024-08-23'),
                          pd.to_datetime('2024-06-23')
                        ],
          })
       }
    }#atom_fits_multiple_eps

    #'name':'ep_fits_multiple_atoms', : Good
  ]
  for m in match_types_testcases:
    has_error = False
    all_ew = []
    name, data = m['name'], m['data']

    print ("\n\t **** testing {name} *** \n\n")
    atoms_df = data['atoms_df']
    episodes_df = data['episodes_df']
    # 
    # not using SLK + Program as RowKey contain assesment date too (more unique)
    atoms_df = merge_key(atoms_df, 'SLK', 'RowKey') 
    episodes_df = merge_key(episodes_df, 'SLK', 'Program')


    slk_program_merged, errors_warnings_matchkey =\
         match_by_matching_keys(episodes_df
                                , atoms_df, matching_keys=['SLK', 'Program'])

    result_matched_df, unmatched_atoms, duplicate_rows_dfs = \
        match_increasing_slack(slk_program_merged, max_slack=error_max_slack)
  
    

    if len(unmatched_atoms) > 0:
      date_matched_errwarns = get_outofbounds_issues(unmatched_atoms, limit_days=warning_limit_days)
      print('unmatched_atoms', unmatched_atoms)
      print ('errors_warnings_matchkey', errors_warnings_matchkey)
      # print ('date_matched_errwarns', date_matched_errwarns)
      has_error = True
      all_ew.extend(date_matched_errwarns)
    
    if len(duplicate_rows_dfs) > 0:
      duplicate_issues = get_duplicate_issues(duplicate_rows_dfs)
      # print('duplicate_issues', duplicate_issues)
      has_error = True
      all_ew.extend(duplicate_issues)
  
    if not has_error:
      print("\t\t ------ great ! --  no date_matched_errwarns\n")
      print("matched df", result_matched_df)
    else:
      print(all_ew)

    print('\n\t\t------------------------------------------\n')

  # from utils.df_ops_base import  get_lr_mux_unmatched
  # left_non_matching, right_non_matching, common_left, common_right = get_lr_mux_unmatched(episodes_df, atoms_df,merge_cols=['SLK', 'Program'])
  # print('left non ' , left_non_matching)
  # print('right non ' , right_non_matching)

  # print(matched_df)


# atoms_df = pd.DataFrame({
  #     'SLK':[
  #        #'1AtStartOfEp', '2BeforeStartOfEp', '3InBetweenEp', '4AtEndOfEp',  '5AfterEp','6AfterEpWarn'
  #        '1MidOfMyEp'#, '1MidOfMyEp', 
  #       #  'GHI',        #  'JKL',        #  'MNO',        #  'PQR'
  #     ],
  #     'RowKey':[
  #        #'rkABC1',      'rkDEF2', 'rkGHI3','rkJKL4',  'rkMNO5','rkPQR6'
  #        'rk1'#, 'rk2',
        
  #     ],
  #     'Program':[#'TSS', 'TSS', 'TSS', 'GOLB','EURO',                'TSS'
  #           'TSS'#, 'TSS'
  #           ],
  #     'AssessmentDate':pd.to_datetime([ #'2023-07-01','2023-09-01', '2023-04-02', '2023-08-23','2024-08-23', '2023-02-26'
  #           '2023-07-01'
  #           #, '2023-08-01'
  #           ])
      
  # })
  # episodes_df = pd.DataFrame({
  #    'SLK':[
  #         #'1AtStartOfEp', '2BeforeStartOfEp', '3InBetweenEp', '4AtEndOfEp','5AfterEp', '6AfterEpWarn'
  #      '1MidOfMyEp', '1MidOfMyEp', 
  #     ],
  #     'Program':[ #'TSS',  'TSS', 'SHAWS', 'GOLB', 'EURO', 'TSS',
  #         'TSS', 'TSS'],
  #     'CommencementDate': pd.to_datetime([
  #                                         #'2023-07-01', '2023-10-01',  '2022-11-01','2022-12-21', '2022-08-23'
  #                                         #'2023-02-28'
  #                                         '2023-06-01', '2023-07-01'
  #                                         ]),
  #     'EndDate': [
  #                  #pd.to_datetime('2023-12-01'), pd.Timestamp.today()
  #                  #, pd.to_datetime('2023-07-01'), pd.to_datetime('2023-08-23'), pd.to_datetime('2023-08-23'),
  #                 # pd.to_datetime('2024-08-23')
  #                 pd.to_datetime('2024-08-23'), pd.to_datetime('2024-06-23')
  #                 ],
  # })

   # Sample DataFrame
  # atoms_df = pd.DataFrame({
  #     'SLK':[
  #        #'1AtStartOfEp', '2BeforeStartOfEp', '3InBetweenEp', '4AtEndOfEp',
  #         '5AfterEp'
  #       #  'GHI',        #  'JKL',        #  'MNO',        #  'PQR'
  #     ],
  #     'RowKey':[
  #       # 'rkABC1',      'rkDEF2', 'rkGHI3','rkJKL4',  
  #        'rkMNO5'
        
  #     ],
  #     'Program':[#'TSS', 'TSS', 'TSS', 'GOLB',
  #                 'EURO'],
  #     'AssessmentDate':pd.to_datetime([ #'2023-07-01','2023-09-01', '2023-04-02', '2023-08-23',
  #                                      '2024-08-23'])
      
  # })
  # episodes_df = pd.DataFrame({
  #    'SLK':[
  #       #  '1AtStartOfEp', '2BeforeStartOfEp', '3InBetweenEp', '4AtEndOfEp',
  #            '5AfterEp'
  #       #  'GHI',        #  'JKL',        #  'MNO',        #  'PQR'
  #     ],
  #     'Program':[ #'TSS',  'TSS', 'SHAWS', 'GOLB',
  #                 'EURO'],
  #     'CommencementDate': pd.to_datetime([
  #                                           #'2023-07-01', '2023-10-01',  '2022-11-01','2022-12-21',
  #                                         '2022-08-23'
  #                                         ]),
  #     'EndDate': [
  #                 # pd.to_datetime('2023-12-01'), pd.Timestamp.today()
  #                 # , pd.to_datetime('2023-07-01'), pd.to_datetime('2023-08-23'),
  #                  pd.to_datetime('2023-08-23')
  #                 ],
  # })