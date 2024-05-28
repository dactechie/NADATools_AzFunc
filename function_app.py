import json
import logging
import azure.functions as func
from matching_helper import match_store_results
# from nada_helper import generate_nada_save,  write_aod_warnings , load_blob_config

# config = load_blob_config()
# if config:
#   print("Config loaded: " ,config.keys())
config = {}
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


# @app.route(route="surveytxt")
# def generate_surveytxt(req: func.HttpRequest) -> func.HttpResponse: # , msg: func.Out[str])
#     logging.info('Called - SurveyTxt Generate. (expects matching to be complete)')
#     start_dt = req.params.get('start_date',"") 
#     end_dt = req.params.get('end_date',"")
#     logging.info(f"Start date , End date {start_dt}  {end_dt}")

#     len_nada, warnings_aod = generate_nada_save(start_dt, end_dt, config)
#     result = {"num_nada_rows": len_nada}

#     if warnings_aod:
#       container_name="atom-matching" 
#       p_str = f"{start_dt}-{end_dt}"
#       outfile = write_aod_warnings(warnings_aod, container_name, period_str=p_str)
#       logging.info(f"Wrote AOD Warnings to {outfile}") 
#       result["warnings_len"] = len(warnings_aod)

#     return func.HttpResponse(body=json.dumps(result),
#                                 mimetype="application/json", status_code=200)


@app.route(route="match")
# @app.function_name(name="HttpTriggerMatching")
def perform_mds_atom_matches(req: func.HttpRequest) -> func.HttpResponse: # , msg: func.Out[str])
    logging.info('Called Match')

    start_dt = req.params.get('start_date',"") 
    end_dt = req.params.get('end_date',"") 
    logging.info(f"Start date , End date {start_dt}  {end_dt}")
    result = match_store_results(start_dt, end_dt, config)
        
    return func.HttpResponse(body=json.dumps({"result": result}),
                                mimetype="application/json", status_code=200)
  