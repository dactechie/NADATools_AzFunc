from datetime import date
import pandas as pd
import utils.df_ops_base as utdf
from mytypes import IssueType, IssueLevel
from mytypes import DataKeys as dk
from configs import audit as audit_cfg

def process_errors_warnings(final_good,  ew:dict, merge_key2
                            ,period_start:date, period_end:date):


    slkonlyin_amst_error, slkprogonlyin_amst_warn = key_matching_errwarn_names(
        merge_key2
        , ew['slk_prog_onlyinass'], IssueType.SLKPROG_ONLY_IN_ASSESSMENT  # warni
        , ew['slk_onlyinass'], IssueType.CLIENT_ONLYIN_ASMT)  # error

    slkonlyin_ep_error, slkprogonlyin_ep_warn = key_matching_errwarn_names(
        merge_key2, ew['slk_prog_onlyin_ep'], IssueType.SLKPROG_ONLY_IN_EPISODE
        , ew['slk_onlyin_ep'], IssueType.CLIENT_ONLYIN_EPISODE)

    # one Assessment matching to multiple episodes
    # duplicate_rows_df = get_dupes_by_key(matched_df, asmt_key)
    # eousides with no assessment in the reporting period
    # mask_matched_eps = ep_asmt_merged_df.PMSEpisodeID.isin(result_matched_df.PMSEpisodeID)

    # previously marked as errors, with the 2nd round of (relaxed i.e. SLK-only matching)
    # we mark them as warnings, as they matches were included in good_df2
    dates_ewdf = ew['dates_ewdf']
    dates_ewdf2 = ew['dates_ewdf2']
    

    dates_ewdf.loc[dates_ewdf.SLK_RowKey.isin(
        final_good.SLK_RowKey), 'issue_level'] = IssueLevel.WARNING.name
    final_dates_ew = pd.concat(
        [dates_ewdf, dates_ewdf2]).reset_index(drop=True)
    
    # date mismatch errors that are outside the reporting period dnt need to be reported.
    a_dt = dk.assessment_date.value
    final_dates_ew = utdf.in_period(final_dates_ew
                      ,a_dt,a_dt
                      ,period_start, period_end)

    # limit columns to write out
    final_dates_ew = final_dates_ew[audit_cfg.COLUMNS_AUDIT_DATES]
    slkonlyin_ep_error = slkonlyin_ep_error[audit_cfg.COLUMNS_AUDIT_EPKEY_CLIENT]
    slkprogonlyin_ep_warn = slkprogonlyin_ep_warn[audit_cfg.COLUMNS_AUDIT_EPKEY_CLIENTPROG]
    slkonlyin_amst_error = slkonlyin_amst_error[audit_cfg.COLUMNS_AUDIT_ASMTKEY]
    slkprogonlyin_amst_warn = slkprogonlyin_amst_warn[audit_cfg.COLUMNS_AUDIT_ASMTKEY]

    # write_validation_results(good_df, dates_ewdf, slk_program_keys_ewdf)
    write_validation_results(final_dates_ew, slkonlyin_amst_error,
                             slkonlyin_ep_error, slkprogonlyin_amst_warn, slkprogonlyin_ep_warn)


    # TODO :Clients without a single ATOM in the period
    #  part of an episode that has an active period in the reporting period
    # - before start of period : may have reported to NADA
    # after end of period :would report to NADA in the next period
    # TODO: see above : filter_atoms_for_matching and date_checks: asmt4clients_w_asmt_onlyoutof_period
    # in_period = good_df[(good_df.AssessmentDate >= reporting_start) & \
    #                     ( good_df.AssessmentDate <= reporting_end )]
    # inperiod_set = set(in_period.SLK.unique())
    # good_c_set = set(good_df.SLK.unique())

    # clients_w_asmts_only_outperiod =  good_df[good_df.SLK.isin(good_c_set - inperiod_set)]
    # print(clients_w_asmts_only_outperiod.SLK.unique())
    # # min(clients_w_zeroasmts_inperiod.AssessmentDate)
        


"""
  #"Key" issues
    # Assessment issues: 
      # not matched to any episode, b/c:
         # assessment SLK not in any episode -> ERROR
         # SLK+Program not in any episode --> WARNING (keep in good dataset)
    # Episode issues:
      # zero assessments, b/c: (note: eps at end of reporting period may not have asmts)
        # episode SLK not in any assessment  -> ERROR 
        # SLK+Program not in Assessment-list -> WARNING (keep in good dataset)
"""


# def key_matching_errors(merge_key: str, slk_prog_onlyin: pd.DataFrame, it1: IssueType,
#                         slk_onlyin: pd.DataFrame,  it2: IssueType):
#     # slk_prog_onlyin (SLK+Program) doesn't need to have anytihng that is also in slk_onlyin (SLK)
#     # redundant
#     slk_prog_onlyin1 = utdf.filter_out_common(
#         slk_prog_onlyin, slk_onlyin, key=merge_key)
#     # mask_common = slk_prog_onlyin[matchkey2].isin(slk_onlyin[matchkey2])
#     slk_prog_warn = slk_prog_onlyin1.assign(
#         issue_type=it1.value,
#         issue_level=IssueLevel.WARNING.value)

#     slk_onlyin_error = slk_onlyin.assign(issue_type=it2.value,
#                                          issue_level=IssueLevel.ERROR.value)
#     # only_in_errors = pd.concat([slk_prog_new, slk_onlyin_new])

#     return slk_onlyin_error, slk_prog_warn


def key_matching_errwarn_names(merge_key: str, slk_prog_onlyin: pd.DataFrame, it1: IssueType,
                        slk_onlyin: pd.DataFrame,  it2: IssueType):
    # slk_prog_onlyin (SLK+Program) doesn't need to have anytihng that is also in slk_onlyin (SLK)
    # redundant
    slk_prog_onlyin1 = utdf.filter_out_common(
        slk_prog_onlyin, slk_onlyin, key=merge_key)
    # mask_common = slk_prog_onlyin[matchkey2].isin(slk_onlyin[matchkey2])
    slk_prog_warn = slk_prog_onlyin1.assign(
        issue_type=it1.name,
        issue_level=IssueLevel.WARNING.name)

    slk_onlyin_error = slk_onlyin.assign(issue_type=it2.name,
                                         issue_level=IssueLevel.ERROR.name)
    # only_in_errors = pd.concat([slk_prog_new, slk_onlyin_new])

    return slk_onlyin_error, slk_prog_warn



def write_validation_results(dates_ewdf: pd.DataFrame
                             , asmt_key_errors: pd.DataFrame
                             , ep_key_errors: pd.DataFrame
                             , asmt_key_warn: pd.DataFrame
                             , ep_key_warn: pd.DataFrame):
    output_folder = 'data/out/errors_warnings/'

    if utdf.has_data(dates_ewdf):
      #utdf.drop_fields(dates_ewdf
      #                , fieldnames='SurveyData') \
      dates_ewdf.to_csv(f'{output_folder}dates_ewdf.csv'
                        , index=False)
    
    if utdf.has_data(asmt_key_errors):
      # utdf.drop_fields(asmt_key_errors
      #                 , fieldnames='SurveyData') \
      asmt_key_errors.to_csv(f'{output_folder}asmt_key_errors.csv'
                            , index=False)

    if utdf.has_data(ep_key_errors):
      ep_key_errors.to_csv(f'{output_folder}ep_key_errors.csv'
                           , index=False)

    # utdf.drop_fields(asmt_key_warn
    #                  , fieldnames='SurveyData') \
    if utdf.has_data(asmt_key_warn):
      asmt_key_warn.to_csv(f'{output_folder}asmt_key_warn.csv'
                           , index=False)

    if utdf.has_data(ep_key_warn):
      ep_key_warn.to_csv(f'{output_folder}ep_key_warn.csv'
                         , index=False)
