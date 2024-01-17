
import pandas as pd
from data_config import EstablishmentID_Program

def prep_for_match(ep_df):
    ep_df['Program'] = ep_df['ESTABLISHMENT IDENTIFIER'].map(EstablishmentID_Program)


def get_stage_per_episode(df:pd.DataFrame)-> pd.Series:  
  df = df.sort_values(by=["PMSEpisodeID", "AssessmentDate"])
  # Rank the assessments within each client
  return  df.groupby('PMSEpisodeID').cumcount()


def get_mask_datefit(row, slack_days=7):
    # Create a Timedelta for slack days
    slack_td = pd.Timedelta(days=slack_days)

    # Check conditions
    after_commencement = row['AssessmentDate'].date() >= (row['CommencementDate'] - slack_td)
    before_end_date = row['AssessmentDate'].date() <= (row['EndDate'] + slack_td)

    return after_commencement and before_end_date


def match_assessments(episodes_df, atoms_df, matching_ndays_slack: int):
    # Merge the dataframes on SLK and Program
    df = pd.merge(episodes_df, atoms_df, how='inner', left_on=[
                  'SLK', 'Program'], right_on=['SLK', 'Program'])

    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    mask = df.apply(get_mask_datefit, slack_days=matching_ndays_slack, axis=1)

    filtered_df = df[mask]
    
    return filtered_df