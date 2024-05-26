

import pandas as pd


from archive.aodsessions_matcher_exporter.exporters.main import AzureBlobExporter
from assessment_episode_matcher.data_prep import prep_dataframe_nada
from assessment_episode_matcher.exporters import NADAbase as out_exporter

def generate_nada_export(matched_assessments:pd.DataFrame, outfile:str):
    container = "atom-matching"
    res, warnings_aod = prep_dataframe_nada(matched_assessments)

    st = out_exporter.generate_finaloutput_df(res)
 
    # exp = AzureBlobExporter(container_name=container) #
    # exp.export_data(data_name=outfile, data=st)    
        
    # return st