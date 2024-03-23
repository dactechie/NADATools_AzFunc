import pandas as pd

from matching.main import filter_good_bad
from models.categories import Purpose
from nada import get_stage_per_episode
from utils.df_xtrct_prep import prep_episodes, extract_atom_data, cols_prep
from utils.dtypes import blank_to_today_str, convert_to_datetime
from data_prep import prep_dataframe_nada
from data_config import nada_final_fields

def final_nada_cols(matched_df):
  df_final = pd.DataFrame(columns=nada_final_fields)

  for column in nada_final_fields:
      if column in matched_df.columns:
          df_final[column] = matched_df[column]  # Or use another default value
      else:
          df_final[column] =""
  return df_final


def prep_surveytxt(df):
  
  df.loc[:,"Stage"] = get_stage_per_episode(df)

  df_final = final_nada_cols(df)
  df_final = cols_prep(df, nada_final_fields, fill_new_cols="")
  df_final['PDCCode'] = df_final['PDCCode'].astype(str).apply(lambda x: x.zfill(4))


  # float_cols = df_final.select_dtypes(include=['float']).columns
  # df_final[float_cols] = df_final[float_cols].astype('Int64')
  return df_final

def main():
  # source_folder = 'data/in/'
  # fname_eps =  f'{source_folder}NSW_MDS_1jan2020-31dec2023.csv'#TEST_NSWMDS.csv'

  # fname_atoms= f'{source_folder}atom_20200106-20240317.parquet' #TEST_ATOM.csv'
  # episode_df = pd.read_csv(fname_eps,   dtype=str)
  
  # episode_df.dropna(subset=['START DATE'], inplace=True)
  # episode_df['END DATE'] = episode_df['END DATE'].apply(lambda x: blank_to_today_str(x))
  # episode_df = prep_episodes(episode_df)

  # atom_df = pd.read_parquet(fname_atoms)
  
  # atom_df = atom_df.rename(columns={'PartitionKey': 'SLK'})
  # # atom_df, warnings = prep_dataframe_matching(atom_df)
  # # assessment_df = assessment_df.rename(columns={'PartitionKey': 'SLK'})
  # atom_df['AssessmentDate'] = convert_to_datetime(atom_df['AssessmentDate'], format='%Y%m%d')
  # # atom_df.dropna(subset=['SurveyData'], inplace=True)
  # validation_issues, good_df, dates_ewdf, slk_program_ewdf = filter_good_bad(episode_df, atom_df)
 
  # res, warnings_aod = prep_dataframe_nada(good_df)
  # res = res[~res.Program.isna()]
  # res.to_csv('data/out/nada.csv',index_label="index")
  res = pd.read_csv('data/out/nada.csv')
  # TODO : Converyt dates to ddmmyyyy
  st = prep_surveytxt(res)
  st.to_csv('data/out/surveytxt.csv', index=False)
  
  return st

if __name__ == "__main__":
    res = main()

