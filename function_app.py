import os
import sys
import json
import logging
import azure.functions as func

from utils.environment import MyEnvironmentConfig
from utils.data_file.InputFileErrors import InputFileError
from utils.data_file.input_file_processor import get_df_from_reqbody
from matching.main import filter_good_bad
from models.categories import Purpose
from utils.df_ops_base import has_data, get_firststart_lastend

from utils.df_xtrct_prep import extract_atom_data, prep_episodes#, df_from_list, cols_prep
from utils.dtypes import make_serializable
from data_prep import prep_dataframe_matching
# from matching.matching import  setup_ep_mergekey,prep_assmt_4match

from datetime import date

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
MyEnvironmentConfig().setup('prod')
"""
  - see full documentation here __ TODO: add docus
  - triggered when a sharepoint file is dropped in folder: ___ TODO: specify __
  - Accepts an MDS-shaped object (Episodes) in the request body (csv, utf-8). Filename can be any
  -  (header row expected? TODO: ? )
  - Gets ATOMs from the start of the earliest episode.
  - Matches episodes to atoms using Matching algorithm and configuration  (in documentation)
  - Generates the following output files in the Sharepoint output folder ____
     * Survey.txt file 
     * Warnings - Episode with no ATOMs
     * Errors   - ATOMs with no Episode
     * Errors with reasons  - Incorrect Program for ATOM/Episode, Out of Episode bounds
     * Warning PDC doesn't match between ATOM and Episode


"""
# @app.route(route="SurveyTxtGenerator")
# # @app.table_input(arg_name="",connection="",table_name="",partition_key="",row_key="",filter="",data_type="")
# def SurveyTxtGenerator(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')
#     conn_str = os.environ["STORAGE_CONNECTION_STRING"]
#     logging.info(f'My app setting value:{conn_str}')

#     warning_limit_days = 3
#     episode_boundary_slack_days = 7
#     try:

#         data = get_data(req)
#         # filename = req.params.get('name') # mds_20230701_20231231.csv
#         # start_date, end_date = get_details_from(data)
#         ep_df = prep_episodes(data)
        
#         # TODO put these in an app.ini/app.cfg on Sharepoint which the logic app loads and passes in as query params
#         # errors_only = False
     
#         # result_dicts = data
#         result_dicts = get_matched_assessments(ep_df, Purpose.NADA # prep df types
#                                                , episode_boundary_slack_days, warning_limit_days)
#         #                                           nostrict=False)
#         df_matched = result_dicts.get("result")
#         # df_warnings = result_dicts.get("warnings")
#         if not df_matched:
#           results = json.dumps({ "result_code":"no_matches"
#                                ,"result_message": result_dicts.get("result_message")
#                                })
#           return func.HttpResponse(body=results,
#                                  mimetype="application/json", status_code=400)
        
#         # generate matching stats , and audit stats
#         # get_matching_stats (df_matched)

#         # save_surveytxt_file (df_matched)

          
#         # in dev env only :
#         # logging.info(result_dicts)
#         results = json.dumps(result_dicts) # how does a DF get converted?

#         return func.HttpResponse(body=results,
#                                  mimetype="application/json", status_code=200)
#     except InputFileError as ife:
#         logging.error(ife)
#         # Using 201  (not appropriate) to distinguish in the LogicApp between error vs non-error state
#         return func.HttpResponse(body=json.dumps({'error': ife.get_msg()}),
#                                  mimetype="application/json", status_code=201)
#     except Exception as e:
#         _, _, exc_traceback = sys.exc_info()
#         logging.exception(e.with_traceback(exc_traceback))
#         return func.HttpResponse(json.dumps({'error': str(e)}), status_code=400)
    
        
        # TODO put these in an app.ini/app.cfg on Sharepoint which the logic app loads and passes in as query params
        # errors_only = False
            # result_dicts = data
@app.route(route="EpisodeAssessmentMatching")
# @app.table_input(arg_name="",connection="",table_name="",partition_key="",row_key="",filter="",data_type="")
def EpisodeAssessmentMatching(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request - MATCHING.')
    conn_str = os.environ["STORAGE_CONNECTION_STRING"]
    logging.info(f'My app setting value:{conn_str}')

    warning_limit_days = 3
    episode_boundary_slack_days = 7
    purpose:Purpose = Purpose.MATCHING

    try:

        ep_df = get_df_from_reqbody(request_body = req.get_body())
        if not has_data(ep_df):
           results = json.dumps({'error': 'No episode data'})
           return func.HttpResponse(body=results, mimetype="application/json", status_code=400)

        
        ep_df = prep_episodes(ep_df)
        # ep_df = merge_keys(ep_df, merge_fields=['SLK', 'Program'])
        
        period_start_dt, period_end_dt = date(2020,1,6), date(2024,3,17)  # atom_20200106-20240317
        #get_firststart_lastend(ep_df['CommencementDate']#   , ep_df['EndDate'])
        
        
        
        atom_df, is_processed = extract_atom_data(period_start_dt, period_end_dt
                                          , purpose=purpose)# NADA->NSW Programs only
        if not has_data(atom_df):
          return func.HttpResponse(body=json.dumps({'error': "No ATOM data"}),
                                mimetype="application/json", status_code=400)
        if not is_processed:
          atom_df = atom_df.rename(columns={'PartitionKey': 'SLK'})
          atom_df, warnings = prep_dataframe_matching(atom_df)
          # atom_df = merge_keys(atom_df, merge_fields=['SLK', 'RowKey'])  
        validation_issues, good_df, ew_df = filter_good_bad(ep_df, atom_df)
                
        ew_df_srlzbl = make_serializable(ew_df, ['CommencementDate', 'EndDate'])

        if not has_data(good_df):
          #  results = json.dumps({'error': 'No matches found'})
          result_type = "No matches found"
        
        else:
          good_df_srlzbl  = make_serializable(good_df, ['CommencementDate', 'EndDate', 'AssessmentDate'])
          result_type = f"{len(good_df)} good results."
                
        results = json.dumps({ "result_type":result_type
                                , "matches": good_df_srlzbl.to_dict()
                                ,"result_data": ew_df_srlzbl.to_dict()
                                ,"errors" : [v.to_dict() for v in validation_issues]
                          })

        return func.HttpResponse(body=results,
                                mimetype="application/json", status_code=200)
          
        # in dev env only :
        # logging.info(result_dicts)

    except InputFileError as ife:
        logging.error(ife)
        # Using 201  (not appropriate) to distinguish in the LogicApp between error vs non-error state
        return func.HttpResponse(body=json.dumps({'error': ife.get_msg()}),
                                 mimetype="application/json", status_code=201)
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        logging.exception(e.with_traceback(exc_traceback))
        return func.HttpResponse(json.dumps({'error': str(e)}), status_code=400)    
    


# def main(csv_data , purpose:Purpose
#                             , episode_boundary_slack_days:int=7
#                             , warning_limit_days:int=3):
  

#   ep_df = epdf_from_list(csv_data)
#   ep_df['Program'] = ep_df['ESTABLISHMENT IDENTIFIER'].map(EstablishmentID_Program)
#   # ep_df = merge_keys(ep_df, merge_fields=['SLK', 'Program'])
  
#   period_start_dt, period_end_dt = get_firststart_lastend(ep_df['CommencementDate']
#                                                             , ep_df['EndDate'])
#           # result_dicts = data
#   atom_df, is_processed = extract_atom_data(period_start_dt, period_end_dt
#                                     , purpose=purpose)# NADA->NSW Programs only
#   if not has_data(atom_df):
#       return None
#   if not is_processed:
#     atom_df = atom_df.rename(columns={'PartitionKey': 'SLK'})
#     # atom_df = merge_keys(atom_df, merge_fields=['SLK', 'RowKey'])  
#   validation_issues, good_df, ew_df = filter_good_bad(ep_df, atom_df)

#   # df_matched, all_ew, has_error = get_matched_assessments(ep_df, atom_df,for_matching # prep df types
#   #                                              , episode_boundary_slack_days, warning_limit_days)
#   print(f"{good_df=}")
#   print(f"{ew_df=}")
#   print(f"{validation_issues=}")


# if __name__ == '__main__':
#   import pandas as pd
#   fname = "./data/test_nsw_mds.csv"
#   df = pd.read_csv(fname)
#   csv = [df.columns.tolist()] +  df.values.tolist()
#   # Convert DataFrame to CSV string
#   # csv_string = df.to_csv(index=False)#, lineterminator='\r\n')
#   # csv_string_custom_breaks = csv_string.replace('\n', custom_line_break)
#   # import csv

#   # def read_csv_to_list(file_path):
#   #     results:list[list[str]] = []
#   #     with open(file_path, mode='r', newline='', encoding='utf-8') as file:
#   #         reader = csv.reader(file, quotechar='"')  # Change quotechar if necessary
#   #         for row in reader:
#   #             # row_string = ', '.join(row)  # Join the row elements into a single string
#   #             results.append(row)
#   #     return results
#     # SurveyTxtGenerator()
#     # data = [] 

#   # data = read_csv_to_list(fname)
#     # data = read_csv_to_dict_list(csv_file_path)
  
#   warning_limit_days = 3
#   episode_boundary_slack_days = 7
#   for_matching:Purpose = Purpose.MATCHING
#   main(csv,for_matching)

#     # name = req.params.get('name')
#     # if not name:
#     #     try:
#     #         req_body = req.get_json()
#     #     except ValueError:
#     #         pass
#     #     else:
#     #         name = req_body.get('name')

#     # if name:
#     #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
#     # else:
#     #     return func.HttpResponse(
#     #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
#     #          status_code=200
#     #     )
 