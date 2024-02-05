import os
import sys
import json
import logging
import azure.functions as func

from utils.environment import MyEnvironmentConfig
from utils.data_file.InputFileErrors import InputFileError
from utils.data_file.input_file_processor import get_data
from survey_from_episodes import prep_episodes
from survey_from_episodes import get_matched_assessments

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
@app.route(route="SurveyTxtGenerator")
# @app.table_input(arg_name="",connection="",table_name="",partition_key="",row_key="",filter="",data_type="")
def SurveyTxtGenerator(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    conn_str = os.environ["STORAGE_CONNECTION_STRING"]
    logging.info(f'My app setting value:{conn_str}')

    episode_boundary_slack_days = 7
    try:

        data = get_data(req)
        # filename = req.params.get('name') # mds_20230701_20231231.csv
        # start_date, end_date = get_details_from(data)
        ep_df = prep_episodes(data)
        
        # TODO put these in an app.ini/app.cfg on Sharepoint which the logic app loads and passes in as query params
        # errors_only = False
     
        # result_dicts = data
        result_dicts = get_matched_assessments(ep_df, episode_boundary_slack_days)
        #                                           nostrict=False)
        df_matched = result_dicts.get("result")
        # df_warnings = result_dicts.get("warnings")
        if not df_matched:
          results = json.dumps({ "result_code":"no_matches"
                               ,"result_message": result_dicts.get("result_message")
                               })
          return func.HttpResponse(body=results,
                                 mimetype="application/json", status_code=400)
        
        # generate matching stats , and audit stats
        # get_matching_stats (df_matched)

        # save_surveytxt_file (df_matched)

          
        # in dev env only :
        # logging.info(result_dicts)
        results = json.dumps(result_dicts)

        return func.HttpResponse(body=results,
                                 mimetype="application/json", status_code=200)
    except InputFileError as ife:
        logging.error(ife)
        # Using 201  (not appropriate) to distinguish in the LogicApp between error vs non-error state
        return func.HttpResponse(body=json.dumps({'error': ife.get_msg()}),
                                 mimetype="application/json", status_code=201)
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        logging.exception(e.with_traceback(exc_traceback))
        return func.HttpResponse(json.dumps({'error': str(e)}), status_code=400)
    

if __name__ == '__main__':
    # SurveyTxtGenerator()
    data = []
    df = prep_episodes(data)

    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )