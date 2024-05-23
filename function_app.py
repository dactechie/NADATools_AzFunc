import os
# import csv
from pathlib import Path
# import sys
import json
import logging
import azure.functions as func

from utils import io
# from assessment_episode_matcher import project_directory
from assessment_episode_matcher.setup.bootstrap import Bootstrap
from matching_helper import get_match_report

# from assessment_episode_matcher.data_prep import prep_dataframe_nada
# from assessment_episode_matcher.exporters import NADAbase as out_exporter


# from datetime import date
home = Path(__file__).parent #os.environ.get("HOME","")
print ("Home path ", home)
print("env envronem home", os.environ.get("HOME",""))
bstrap = Bootstrap.setup(Path(home), env="dev")
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
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

@app.route(route="base")
# @app.table_input(arg_name="",connection="",table_name="",partition_key="",row_key="",filter="",data_type="")
def BaseTest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request - MATCHING.')
    print("bootstrap", bstrap)
    # cache_file_path = bstrap.in_dir / 'cache_file.txt'
    # cache_file_path.touch()
    return func.HttpResponse(body=json.dumps({"result":"ok"}),
                                mimetype="application/json", status_code=200)


@app.function_name(name="EpisodesBlobTriggerFunc")
@app.blob_trigger(arg_name="episodes", path="atom-matching/MDS/{name}", connection="AzureWebJobsStorage")
def main(episodes: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {episodes.name}\n"
                 f"Blob Size: {episodes.length} bytes")
    
    if not episodes.name:
      filename = Path("test.csv")
    else:
      filename = Path(f"{episodes.name}") # atom-matching\\MDS\\filename.csv

    
    # Process the CSV file data here
    csv_from_stream = episodes.read().decode('utf-8-sig')

    io.write_stream_to_csv(bstrap.in_dir, f"T_{filename.name}" , csv_from_stream)
    # Perform operations with the CSV data
    # ...
    # print("csv dta", csv_from_stream)
    logging.info("CSV file processing completed.")
#     except InputFileError as ife:
#         logging.error(ife)
#         # Using 201  (not appropriate) to distinguish in the LogicApp between error vs non-error state
#         return func.HttpResponse(body=json.dumps({'error': ife.get_msg()}),
#                                  mimetype="application/json", status_code=201)
#     except Exception as e:
#         _, _, exc_traceback = sys.exc_info()
#         logging.exception(e.with_traceback(exc_traceback))
#         return func.HttpResponse(json.dumps({'error': str(e)}), status_code=400) 
    


@app.route(route="match")
@app.function_name(name="HttpTriggerMatching")
def perform_mds_atom_matches(req: func.HttpRequest) -> str: # , msg: func.Out[str])
    logging.info('Python HTTP trigger function processed a request - MATCHING.')

    print("bootstrap", bstrap)

    start_dt = req.params.get('start_date',"") 
    end_dt = req.params.get('end_date',"") 
    purpose = req.params.get('purpose', "NADA")    
    result = get_match_report(start_dt, end_dt)
    return result




# def perform_mds_atom_matches(req: func.HttpRequest) -> str: # , msg: func.Out[str])
#     logging.info('Python HTTP trigger function processed a request - MATCHING.')

#     print("bootstrap", bstrap)
#     cfg, logger = bstrap.config, bstrap.logger    
#     connection_string = str(cfg.get(ConfigKeys.AZURE_STORAGE_CONNECTION_STRING.value,""))
#     print(connection_string)

#     slack_for_matching = int(cfg.get(ConfigKeys.MATCHING_NDAYS_SLACK.value, 7))

#     start_dt = req.params.get('start_date',"") 
#     end_dt = req.params.get('end_date',"") 
#     purpose = req.params.get('purpose', "NADA")
    
#     # df = EpisodesImporter.import_data(eps_st=start_dt, eps_end="",prefix="MDS", suffix="AllPrograms")
#     episode_df = EpisodesImporter.import_data(
#                             start_dt, end_dt
#                             , prefix="MDS", suffix="AllPrograms")
#     if not utdf.has_data(episode_df):
#       logging.error("No episodes")
#       return json.dumps({"result":"no episode data"})
#                         # func.HttpResponse(body=json.dumps({"result":"no episode data"}),
#                         #         mimetype="application/json", status_code=200)    
    
#     atoms_df = ATOMsImporter.import_data(
#                             start_dt, end_dt
#                             , purpose=Purpose.NADA, refresh=True)
#                             # , prefix="MDS", suffix="AllPrograms")
#     if not utdf.has_data(atoms_df):
#       logging.error("No ATOMs")
#       return json.dumps({"result":"no ATOM data"})
#       # func.HttpResponse(body=json.dumps({"result":"no ATOM data"}),
#       #                           mimetype="application/json", status_code=200) 
#     reporting_start, reporting_end = get_date_from_str (start_dt,"%Y%m%d") \
#                                   , get_date_from_str (end_dt,"%Y%m%d")
#     a_df, e_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom = \
#       match_helper.get_data_for_matching2(episode_df, atoms_df
#                                         , reporting_start, reporting_end, slack_for_matching=7)

#     if not utdf.has_data(a_df) or not utdf.has_data(e_df):
#       print("No data to match. Ending")
#       logging.error("No ATOMs")
#       return json.dumps({"result":"no ATOM data"})        
#         # return func.HttpResponse(body=json.dumps({"result":"no data"}),
#         #                         mimetype="application/json", status_code=200)    
#     # e_df.to_csv('data/out/active_episodes.csv')
#     final_good, ew = match_helper.match_and_get_issues(e_df, a_df
#                                           , inperiod_atomslk_notin_ep
#                                           , inperiod_epslk_notin_atom, slack_for_matching)

#     warning_asmt_ids  = final_good.SLK_RowKey.unique()
    

#     #TODO : #AutidExporter should have write to blob function
#     # aexptr = AuditExporter(config={'location' : f'{bstrap.ew_dir}'})

#     # process_errors_warnings(ew, warning_asmt_ids, dk.client_id.value
#     #                         , period_start=start_dt
#     #                         , period_end=end_dt
#     #                         , audit_exporter=aexptr)
  

#     # df_reindexed = final_good.reset_index(drop=True)
#     # df_reindexed.to_csv(f'{bstrap.out_dir}/reindexed.csv', index_label="index")

#     # rexptr.export(df_reindexed)    #TODO : push df_reindexed to blob

#     # # return df_reindexed

#     # msg.set(json.dumps({"result":df_reindexed})
#     # # cache_file_path = bstrap.in_dir / 'cache_file.txt'
#     # # cache_file_path.touch()
#     return "ok"
#     # func.HttpResponse(body=json.dumps({"result":df_reindexed}),
#                                 # mimetype="application/json", status_code=200)

