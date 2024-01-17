import os
import azure.functions as func
import logging

from utils.data_file.InputFileErrors import InputFileError
from utils.data_file.input_file_processor import get_details_from, get_data
from survey_from_episodes import get_matched_assessments

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="SurveyTxtGenerator")
# @app.table_input(arg_name="",connection="",table_name="",partition_key="",row_key="",filter="",data_type="")
def SurveyTxtGenerator(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    conn_str = os.environ["STORAGE_CONNECTION_STRING"]
    logging.info(f'My app setting value:{conn_str}')

    episode_boundary_slack_days = 7
    try:

        data = get_data(req)
        filename = req.params.get('name') # mds_20230701_20231231.csv
        start_date, end_date = get_details_from(filename)

        # TODO put these in an app.ini/app.cfg on Sharepoint which the logic app loads and passes in as query params
        errors_only = False
     

        result_dicts = get_matched_assessments(start_date,end_date, data, episode_boundary_slack_days
                                                
                                                  nostrict=False)
        # in dev env only :
        # logging.info(result_dicts)
        results = json.dumps(result_dicts)

        return func.HttpResponse(body=results,
                                 mimetype="application/json", status_code=200)
    except InputFileError as ife:
        logger.error(ife)
        # Using 201  (not appropriate) to distinguish in the LogicApp between error vs non-error state
        return func.HttpResponse(body=json.dumps({'error': ife.get_msg()}),
                                 mimetype="application/json", status_code=201)
    except Exception as e:
        _, _, exc_traceback = sys.exc_info()
        logger.exception(e.with_traceback(exc_traceback))
        return func.HttpResponse(json.dumps({'error': str(e)}), status_code=400)
    

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