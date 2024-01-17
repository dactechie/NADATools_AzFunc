

from utils.df_xtrct_prep import extract_prep_atom_data, load_and_parse_csv
# from utils.fromstr import convert_format_datestr
from matching import match_assessments, get_stage_per_episode, prep_for_match
from data_config import nada_final_fields



def prep_for_surveytxt(ep_df):
    ep_df.rename(columns={'ESTABLISHMENT IDENTIFIER': 'AgencyCode'}, inplace=True)

def prep_episodes(ep_data):
  # List of columns we care about
  columns_of_interest = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION', 'EPISODE ID','PERSON ID', 'SPECIFY DRUG OF CONCERN', 'PRINCIPAL DRUG OF CONCERN', 'START DATE', 'END DATE', 'SLK']
  rename_columns = {
      'SPECIFY DRUG OF CONCERN': 'PDCSubstanceOfConcern',
      'PRINCIPAL DRUG OF CONCERN': 'PDCCode',
      'START DATE': 'CommencementDate', 'END DATE': 'EndDate',
      'EPISODE ID': 'PMSEpisodeID', 'PERSON ID': 'PMSPersonID',    
  }
  ep_df = load_and_parse_csv(ep_data, rename_columns, columns_of_interest, date_cols=['START DATE', 'END DATE'])
  return ep_df


def final_nada_cols(matched_df):
  df_final = pd.DataFrame(columns=nada_final_fields)

  for column in nada_final_fields:
      if column in matched_df.columns:
          df_final[column] = matched_df[column]  # Or use another default value
      else:
          df_final[column] =""
  return df_final


def get_matched_assessments(period_start_dt: str
                            , period_end_dt: str
                            , episode_data
                            , episode_boundary_slack_days=7):
    #log len(ep_df), min(ep_df.CommencementDate), max(ep_df.CommencementDate)
    ep_df = prep_episodes(episode_data)
    ep_df = prep_for_match(ep_df)

    # get ATOMs from DB
    atom_df = extract_prep_atom_data(extract_start_date, extract_end_date
                                 , active_clients_start_date
                                 , active_clients_end_date
                                 , fname, purpose='NADA')
        
    # prep for match

    # do matching
    # note anomalies
      # - no atoms for episode
      # - no episodes for atom
    matched_df = match_assessments(ep_df, atom_df, episode_boundary_slack_days)

    matched_df["Stage"] = get_stage_per_episode(matched_df)

    df_final = final_nada_cols(matched_df)

    float_cols = df_final.select_dtypes(include=['float']).columns
    df_final[float_cols] = df_final[float_cols].astype('Int64')

    return None
