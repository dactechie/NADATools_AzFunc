import pandas as pd
from data_prep import prep_dataframe_matching
from matching.main import filter_good_bad
from models.categories import Purpose
from utils.df_xtrct_prep import prep_episodes, extract_atom_data
from utils.dtypes import blank_to_today_str, convert_to_datetime
# from test_data import episode_data, assessment_data
# def main():
#     purpose:Purpose = Purpose.MATCHING
#     # episode_df = pd.DataFrame(episode_data)
#     # assessment_df = pd.DataFrame(assessment_data)
#     episode_df = pd.read_csv('data/in/TEST_NSWMDS_1Dec2022_20Feb2023_ddmmyyyy.csv')
#     episode_df = prep_episodes(episode_df)
#     atom_df = pd.read_parquet('data/in/atom_20211216-20231005.parquet', 
#                               dtypes=str)
    
#     # atom_df, is_processed = extract_atom_data(20211216, 20231005
#     #                                       , purpose=purpose)
#     atom_df = atom_df.rename(columns={'PartitionKey': 'SLK'})
#     validation_issues, good_df, ew_df = filter_good_bad(episode_df, atom_df)
#     ew_df.to_csv('data/out/ew_df.csv')
   


# if __name__ == "__main__":
#     main()


# # from test_data import episode_data, assessment_data
def main1():
    # episode_df = pd.DataFrame(episode_data)
    # assessment_df = pd.DataFrame(assessment_data)
    source_folder = 'data/in/'
    fname_eps =  f'{source_folder}NSW_MDS_1jan2020-31dec2023.csv'#TEST_NSWMDS.csv'

    fname_atoms= f'{source_folder}atom_20200106-20240317.parquet' #TEST_ATOM.csv'
    episode_df = pd.read_csv(fname_eps,   dtype=str)
    
    episode_df.dropna(subset=['START DATE'], inplace=True)
    episode_df['END DATE'] = episode_df['END DATE'].apply(lambda x: blank_to_today_str(x))
    episode_df = prep_episodes(episode_df)

    atom_df = pd.read_parquet(fname_atoms)
    
    atom_df = atom_df.rename(columns={'PartitionKey': 'SLK'})
    # atom_df, warnings = prep_dataframe_matching(atom_df)
    # assessment_df = assessment_df.rename(columns={'PartitionKey': 'SLK'})
    atom_df['AssessmentDate'] = convert_to_datetime(atom_df['AssessmentDate'], format='%Y%m%d')
    atom_df.dropna(subset=['SurveyData'], inplace=True)
    validation_issues, good_df, dates_ewdf, slk_program_ewdf = filter_good_bad(episode_df, atom_df)

    vi = pd.DataFrame(validation_issues)
    vi.to_csv('data/out/validation_issues.csv')

    dates_ewdf.drop('SurveyData', axis=1, inplace=True)
    dates_ewdf.to_csv('data/out/dates_ewdf.csv')
   
    slk_program_ewdf.drop('SurveyData', axis=1, inplace=True)
    slk_program_ewdf.to_csv('data/out/slk_program_ewdf.csv')
    print("Done")


def main(): #WHY IS THIS SLK not there !!# entity_chosen['PartitionKey'] == 'IR2HR040719671'
  from datetime import date
  from utils.environment import MyEnvironmentConfig
  MyEnvironmentConfig().setup('prod')
  period_start_dt, period_end_dt = date(2022,7,2), date(2023,3,31)  # atom_20200106-20240317
        #get_firststart_lastend(ep_df['CommencementDate']#   , ep_df['EndDate'])
        
        
        
  atom_df, is_processed = extract_atom_data(period_start_dt, period_end_dt
                                    , purpose=Purpose.MATCHING)# NADA->NSW Programs only   

  print(atom_df) 

if __name__ == "__main__":
    main1()
