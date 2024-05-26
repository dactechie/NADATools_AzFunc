# import os
# import csv
from pathlib import Path
# import sys
import json
import logging
import azure.functions as func

# import ptvsd
# ptvsd.enable_attach(address=('0.0.0.0', 5678))
# ptvsd.wait_for_attach()
# from utils import io
# from assessment_episode_matcher import project_directory
from assessment_episode_matcher.setup.bootstrap import Bootstrap
from matching_helper import get_match_report
from nada_helper import generate_nada_export

# from assessment_episode_matcher.data_prep import prep_dataframe_nada
# from assessment_episode_matcher.exporters import NADAbase as out_exporter


# from datetime import date
home = Path(__file__).parent #os.environ.get("HOME","")
print("Going to do setup via boostrap")
bstrap = Bootstrap.setup(Path(home), env="prod")
# print("Done Setup")
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# print ("Home path ", home)
# print("env envronem home", os.environ.get("HOME",""))
# ConfigManager().setup('dev')
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
        
        # TODO put these in an app.ini/app.cfg on Sharepoint which the logic app loads and passes in as query params
        # errors_only = False
            # result_dicts = data
# @app.table_input(arg_name="",connection="",table_name="",partition_key="",row_key="",filter="",data_type="")
@app.route(route="base")
def BaseTest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request - MATCHING.')

    return func.HttpResponse(body=json.dumps({"result":"ok"}),
                                mimetype="application/json", status_code=200)


# @app.route(route="surveytxt")
# # @app.function_name(name="HttpTriggerMatching")
# def generate_surveytxt(req: func.HttpRequest) -> func.HttpResponse: # , msg: func.Out[str])
#     logging.info('Python HTTP trigger function processed a request - MATCHING.')
#     start_dt = req.params.get('start_date',"") 
#     end_dt = req.params.get('end_date',"")
#     matched = get_matched_assessments()
#     generate_nada_export(matched, f"NADA/{start_dt}-{end_dt}.parquet")

#     return func.HttpResponse(body=json.dumps({"result": result}),
#                                 mimetype="application/json", status_code=200)

@app.route(route="match")
# @app.function_name(name="HttpTriggerMatching")
def perform_mds_atom_matches(req: func.HttpRequest) -> func.HttpResponse: # , msg: func.Out[str])
    logging.info('Python HTTP trigger function processed a request - MATCHING.')

    # print("bootstrap", bstrap)
    print("helo")
    start_dt = req.params.get('start_date',"") 
    end_dt = req.params.get('end_date',"") 
    logging.info(f"Srart date , End date {start_dt}  {end_dt}")
    result = get_match_report(start_dt, end_dt)
    
    
    return func.HttpResponse(body=json.dumps({"result": result}),
                                mimetype="application/json", status_code=200)
  