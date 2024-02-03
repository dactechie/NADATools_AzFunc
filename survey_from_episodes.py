
from utils.df_xtrct_prep import extract_prep_atom_data, df_from_list, cols_prep
from utils.df_ops_base import get_firststart_lastend
# from utils.fromstr import convert_format_datestr
from matching import match_assessments, get_stage_per_episode, prep_for_match
from data_config import nada_final_fields



def prep_for_surveytxt(ep_df):
    ep_df.rename(columns={'ESTABLISHMENT_IDENTIFIER': 'AgencyCode'}, inplace=True)

def prep_episodes(ep_data:list[str]):
  # List of columns we care about
  columns_of_interest = ['ESTABLISHMENT_IDENTIFIER', 'GEOGRAPHICAL_LOCATION'
                         , 'EPISODE_ID','PERSON_ID', 'SPECIFY_DRUG_OF_CONCERN'
                        #  , 'PRINCIPAL DRUG OF CONCERN'
                         , 'START_DATE', 'END_DATE', 'SLK']
  rename_columns = {
      'SPECIFY_DRUG_OF_CONCERN': 'PDCSubstanceOfConcern',
   #   'PRINCIPAL DRUG OF CONCERN': 'PDCCode',
      'START_DATE': 'CommencementDate', 'END_DATE': 'EndDate',
      'EPISODE_ID': 'PMSEpisodeID', 'PERSON_ID': 'PMSPersonID',    
  }
  ep_df = df_from_list(ep_data, rename_columns, columns_of_interest, date_cols=['START_DATE', 'END_DATE'])
  return ep_df



def get_matched_assessments( episode_data
                            , episode_boundary_slack_days=7):
    #log len(ep_df), min(ep_df.CommencementDate), max(ep_df.CommencementDate)
    # ep_df = prep_episodes(episode_data)
    ep_df = prep_for_match(episode_data)
    period_start_dt, period_end_dt = get_firststart_lastend(ep_df['CommencementDate']
                                                            , ep_df['EndDate'])

    # get ATOMs from DB
    atom_df = extract_prep_atom_data(period_start_dt, period_end_dt
                                    , purpose='NADA')
        
    # prep for match

    # do matching
    # note anomalies
      # - no atoms for episode
      # - no episodes for atom
    matched_df = match_assessments(ep_df, atom_df, episode_boundary_slack_days)

    matched_df["Stage"] = get_stage_per_episode(matched_df)

    df_final = cols_prep(matched_df, nada_final_fields, fill_new_cols="")

    float_cols = df_final.select_dtypes(include=['float']).columns
    df_final[float_cols] = df_final[float_cols].astype('Int64')

    return None
