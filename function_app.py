import json
import logging
import azure.functions as func
import utils.log_telemetry as log_telemetry
import matching_helper as ATOMEpisodeMatcher
import nada_helper as NADAImportFileGenerator

log_telemetry.setup_logger_with_app_insights("NADA_Tools_Logger")
# if config:
#   print("Config loaded: " ,config.keys())
# config = load_blob_config()
# Bootstrap.setup(Path(home), env="prod")

# print("Done Setup")
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

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

# @app.table_input(arg_name="",connection="",table_name="",partition_key="",row_key="",filter="",data_type="")
@app.route(route="base")
def BaseTest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request - MATCHING.')

    return func.HttpResponse(body=json.dumps({"result":"ok"}),
                                mimetype="application/json", status_code=200)


@app.route(route="surveytxt")
def generate_surveytxt(req: func.HttpRequest) -> func.HttpResponse: # , msg: func.Out[str])
    try:
      logging.info('Called - SurveyTxt Generate. (expects matching to be complete)')
      start_dt = req.params.get('start_date',"") 
      end_dt = req.params.get('end_date',"")
      logging.info(f"Start date , End date {start_dt}  {end_dt}")

      result = NADAImportFileGenerator.run(start_yyyymmd=start_dt
                                          ,end_yyyymmd=end_dt)
      logging.info('Completed - SurveyTxt Generate.')

      return func.HttpResponse(body=json.dumps(result),
                                  mimetype="application/json", status_code=200)
    except Exception as exp:
        logging.exception("Exception raised while processing generate_surveytxt", exp)
        return func.HttpResponse(body=json.dumps(exp),
                                  mimetype="application/json", status_code=400)
        
        


@app.route(route="match")
# @app.function_name(name="HttpTriggerMatching")
def perform_mds_atom_matches(req: func.HttpRequest) -> func.HttpResponse: # , msg: func.Out[str])
    try:
      logging.info('Called Match')

      start_dt = req.params.get('start_date',"") 
      end_dt = req.params.get('end_date',"")  
      nearest_slk = int(req.params.get('nearest_slk', "0"))
      
      logging.info(f"Start date , End date {start_dt}  {end_dt}")
      
      result = ATOMEpisodeMatcher.run(start_yyyymmd=start_dt
                                      ,end_yyyymmd=end_dt
                                      , get_nearest_matching_slk=nearest_slk)
      
      logging.info('Completed Match')

      return func.HttpResponse(body=json.dumps(result),
                                  mimetype="application/json", status_code=200)

    except AttributeError as ae:
        logging.exception("AttributeError raised while processing perform_mds_atom_matches", ae.args)
        return func.HttpResponse(body=json.dumps(ae),
                                  mimetype="application/json", status_code=400) 
    except Exception as exp:
        logging.exception("Exception raised while processing perform_mds_atom_matches", exp)
        return func.HttpResponse(body=json.dumps(exp),
                                  mimetype="application/json", status_code=400)
  