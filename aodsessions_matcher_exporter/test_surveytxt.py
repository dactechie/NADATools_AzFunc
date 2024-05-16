import logging
from datetime import date
import pandas as pd
# from utils.base import get_period_range
# from utils.io import write_parquet
# from azutil.helper import get_fresh_data_only

from utils.environment import ConfigManager, ConfigKeys
from matching.main import get_data_for_matching, \
  perform_date_matches, get_merged_for_matching, filter_asmt_by_ep_programs

from matching.errors import process_errors_warnings
from mytypes import DataKeys as dk
# from models.categories import Purpose
# from utils.df_xtrct_prep import prep_episodes #, extract_atom_data, cols_prep
# from utils.dtypes import blank_to_today_str, convert_to_datetime
from data_prep import prep_dataframe_nada

from exporters import NADAbase as out_exporter
from importers import episodes as imptr_episodes
from importers import assessments as imptr_atoms

import utils.df_ops_base as utdf
from mytypes import DataKeys as dk


def do_matches_slkprog(a_ineprogs:pd.DataFrame, e_df:pd.DataFrame, slack_for_matching:int) \
           -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    mkeys = ['SLK', 'Program']
    merged_df, merge_key, match_key, slk_prog_onlyinass, slk_prog_onlyin_ep = \
        get_merged_for_matching(e_df, a_ineprogs, mergekeys_to_check=mkeys
                                , match_keys=[dk.episode_id.value, dk.assessment_id.value]
                                )
    good_df, dates_ewdf = perform_date_matches(
        merged_df, match_key, slack_ndays=slack_for_matching)
    # exclude already matched assessments
    # len(a_df) should be = len(merged_df) + len(slk_prog_onlyinass)
    return good_df, dates_ewdf, slk_prog_onlyinass, slk_prog_onlyin_ep 
    

def do_matches_slk(not_matched_asmts_slkprog:pd.DataFrame, e_df:pd.DataFrame, slack_for_matching:int) \
           -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
  
    # retry mismatching dates, with just SLK
    # (in case the assesssment was made in a program different to the episode program)
    mkeys = ['SLK']
    
    # a_ineprogs , a_notin_eprogs = filter_asmt_by_ep_programs (e_df, not_matched_asmts)
    merged_df3, merge_key2 \
      , match_key3, slk_onlyinass, _ = get_merged_for_matching(
                      e_df, not_matched_asmts_slkprog
                      , mergekeys_to_check=mkeys
                      , match_keys=[dk.episode_id.value, dk.assessment_id.value])
    # try date-matching again, but only where the SLKs are same but the Programs are different
    merged_df4 = merged_df3[merged_df3['Program_x'] != merged_df3['Program_y']]
    good_df2, dates_ewdf2 = perform_date_matches(
        merged_df4, match_key3, slack_ndays=slack_for_matching)
    
    return good_df2, dates_ewdf2, slk_onlyinass, merge_key2


def fix_incorrect_program(slkprog_datematched:pd.DataFrame, slk_datematched:pd.DataFrame):
    # program may be different for this client,asessment,  but highlight unlikely to be on the same day, so exclude :
    # (-8 - asmt date)
    slk_datematched['Ep_AsDate'] = slk_datematched['SLK']+'_' + slk_datematched.PMSEpisodeID + \
        '_' + slk_datematched.PMSEpisodeID_SLK_RowKey.str[-8:]
    slkprog_datematched['Ep_AsDate'] = slkprog_datematched['SLK']+'_' + slkprog_datematched.PMSEpisodeID + \
        '_' + slkprog_datematched.PMSEpisodeID_SLK_RowKey.str[-8:]

    # good_df2[~good_df2.Ep_AsDate.isin(good_df2_v2.Ep_AsDate)]  # AZKND150719831 (19/6/2023)  RIGAM080820061 (26/9/2023)
    
    # keep only what is in slk_datematched
    slk_datematched_v2 = utdf.filter_out_common(slk_datematched, slkprog_datematched, key='Ep_AsDate')
    
    # good_df2_v2 =  good_df2[~good_df2.Ep_AsDate.isin(good_df.Ep_AsDate)]
    print("fix_incorrect_program: moving rows from slk_datematched becauase they exist in slkprog_datematched: \n"
          , slk_datematched[~slk_datematched.Ep_AsDate.isin(slk_datematched_v2.Ep_AsDate)])
    slk_datematched_v2 = utdf.drop_fields(slk_datematched_v2, ['Ep_AsDate'])
    # for conflicts in Program , stamp the program of the episode
    slk_datematched_v2['Program'] = slk_datematched_v2['Program_y']
    # ESTBLISHmentID or Program_y has the right program (frm the matched episode)
    return slkprog_datematched, slk_datematched_v2


def main2():
    ConfigManager.setup('dev')
    cfg = ConfigManager().config
    slack_for_matching = int(cfg.get(ConfigKeys.MATCHING_NDAYS_SLACK, 7))
    refresh_assessments = False #cfg.get( ConfigKeys.REFRESH_ATOM_DATA, True )
    reporting_start = date(2024, 1, 1)
    reporting_end = date(2024, 3, 31)
    # source_folder = 'data/in/'
    eps_st = '20220101'
    eps_end = '20240331'
    # eps_st ='20220101'
    # eps_end= '20240331'
    asmt_st, asmt_end = "20160701",  "20240508"

    # # FIX ME: multiple atoms on the same day EACAR171119722 16/1/2024

    a_df, e_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom = \
        get_data_for_matching( imptr_episodes \
                                       , imptr_atoms \
                                       , eps_st, eps_end \
                                       , reporting_start, reporting_end \
                                       , assessment_start=asmt_st, assessment_end=asmt_end \
                                       , slack_for_matching=slack_for_matching \
                                       , refresh=refresh_assessments
                                      )
    if not utdf.has_data(a_df) or not utdf.has_data(e_df):
        print("No data to match. Ending")
        return None    
    e_df.to_csv('data/out/active_episodes.csv')

    # SLK_RowKey
    a_df, _ = utdf.merge_keys(
        a_df, [dk.client_id.value, dk.per_client_asmt_id.value])

    # a_ineprogs = a_df
    a_ineprogs , a_notin_eprogs = filter_asmt_by_ep_programs (e_df, a_df)
    # print(f"Assessments not in any of the programs of the episode {len(a_notin_eprogs)}")

    # XXA this assumes Assessment's program is always Correct 
    slkprog_datematched, dates_ewdf , slk_prog_onlyinass, slk_prog_onlyin_ep  = do_matches_slkprog(a_ineprogs 
                                                            , e_df
                                                            , slack_for_matching
                                                            )
    a_key = dk.assessment_id.value # SLK +RowKey
    # ATOMs that could not be date matched with episode, when merging on SLK+Program
    unmatched_asmt_by_slkprog = a_ineprogs[~a_ineprogs[a_key].isin(
        slkprog_datematched[a_key].unique())]

    slkonly_datematched, dates_ewdf2, slk_onlyinass, merge_key2  = do_matches_slk(unmatched_asmt_by_slkprog 
                                                            , e_df
                                                            , slack_for_matching
                                                            )
    # XXA if the assessment's program was not correct, it would have only matched by SLK i.e. in slkonly_datematched not in good_df
    # try to change the matching key to SLK+EpisodeID+AssessmentDate (no program) and use the program of the episode
    # then 'move' the assessment from slkonly_datematched to slkprog_datematched
    slkprog_datematched, slkonly_datematched_v2 = fix_incorrect_program(slkprog_datematched, slkonly_datematched)
    final_good = pd.concat([slkprog_datematched, slkonly_datematched_v2])
    
    # can't use the result above (_)as we are only using the un-merged assesemtns (slk_prog_onlyinass) as input
    # slk_onlyin_ep = e_df[e_df.SLK.isin(a_inepregs.SLK.unique())]
    slk_onlyin_ep = utdf.filter_out_common(e_df, a_ineprogs, key='SLK')

    # TODO: explain why these are two different things (pre date-matching vs post date-matching errors)
    print("concating pre-match missing SLK errors of lengths :")
    print(f"\n\t only-in-ATOM: {len(inperiod_atomslk_notin_ep)}  ; only in Episode: {len(inperiod_epslk_notin_atom)} ")
    slk_onlyinass = pd.concat([slk_onlyinass, inperiod_atomslk_notin_ep])
    slk_onlyin_ep = pd.concat([slk_onlyin_ep, inperiod_epslk_notin_atom])

    ew = {
        'slk_onlyinass': slk_onlyinass,
        'slk_onlyin_ep': slk_onlyin_ep,
        'slk_prog_onlyinass': slk_prog_onlyinass,
        'slk_prog_onlyin_ep': slk_prog_onlyin_ep,
        'dates_ewdf': dates_ewdf,
        'dates_ewdf2': dates_ewdf2
    }

    process_errors_warnings(final_good, ew, merge_key2
                            , period_start=reporting_start
                            , period_end=reporting_end)
  

    df_reindexed = final_good.reset_index(drop=True)
    df_reindexed.to_csv('data/out/reindexed.csv', index_label="index")

    res, warnings_aod = prep_dataframe_nada(df_reindexed)

    st = out_exporter.generate_finaloutput_df(res)
    # st.to_parquet('/data/out/surveytxt.parquet')
    st.to_csv(
        f'data/out/{reporting_start}_{reporting_end}_surveytxt.csv', index=False)

    return st



if __name__ == "__main__":
    res = main2()


# if __name__ == '__main__':
#     from utils import io
#     df1 = pd.DataFrame({'PartitionKey': ['A', 'B', 'C', 'E'],
#                         'RowKey': [1, 2, 3, 4],
#                         'Value': [10, 20, 30, 15],
#                         # 'IsActive': [1,1,1,1]

#                         })
#     df2 = pd.DataFrame({'PartitionKey': ['B', 'C', 'D', 'E'],
#                         'RowKey': [2, 3, 4, 4],
#                         'Value': [40, 50, 60, 15],
#                            'IsActive': [1,1,1,0]})

#     merged_df = io.refresh_dataset(df1, df2)
#     print(merged_df)


# def main ():
#   MyEnvironmentConfig.setup('dev')

#   # asmt_st, asmt_end = "20190701",  "20240331"
#   asmt_st, asmt_end = "20150101",  "20240411"
#   atom_df = imptr_atoms.import_data(asmt_st, asmt_end, purpose=Purpose.NADA, refresh=True)
#   return atom_df
