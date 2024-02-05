# from utils.environment import MyEnvironmentConfig
# from azure.data.tables import  TableEntity
from .az_tables_query import SampleTablesQuery
# import mylogger
# logger = mylogger.get(__name__)
# from data_config import survey_datacols
# from utils.fromstr import get_date_from_yyyymmdd

# config = MyEnvironmentConfig()

# def get_json_result(data:TableEntity, fields:list) -> dict:
     
      #survey_data:dict = json.loads(atom.get("SurveyData",{}))
      # result  = {
      #     "PartitionKey": atom.get("PartitionKey"),
      #     "RowKey" : atom.get("RowKey"),
      #     "Program": atom.get("Program"),
      #     "Staff": atom.get("Staff"),
      #     "SurveyName": atom.get("SurveyName"),
      #     "SurveyData": atom.get("SurveyData",{})
      #     #"SurveyData": survey_data
      # }
      # return result

table_config = {
  'ATOM':{
       "fields": [u"PartitionKey", u"RowKey", u"Program", u"Staff", u"SurveyName", u"SurveyData"],
       
       "filter":  u"AssessmentDate ge @lower and AssessmentDate lt @upper and IsActive eq 1 and Program ne 'TEST' and Status eq 'Complete'"
  },
  'MDS':{
       "fields":['PartitionKey',	'GEOGRAPHICAL_LOCATION',	'RowKey',	'SLK'
                 ,	'PERSON_ID',	'DOB',	'DOB_STATUS',	'SEX',	'COUNTRY_OF_BIRTH',	'INDIGENOUS_STATUS',	'PREFERRED_LANGUAGE'
                 ,	'SOURCE_OF_INCOME',	'LIVING_ARRANGEMENT',	'USUAL_ACCOMMODATION',	'CLIENT_TYPE',	'PRINCIPAL_DRUG_OF_CONCERN',
                    	'SPECIFY_DRUG_OF_CONCERN',	'ILLICIT_USE',	'METHOD_OF_USE_PRINCIPAL_DRUG',	'INJECTING_DRUG_USE',	'START_DATE',
                         	'POSTCODE',	'SOURCE_OF_REFERRAL',	'MAIN_SERVICE',	'END_DATE',	'END_REASON',	'REFERRAL_TO_ANOTHER_SERVICE'],
        "filter":  "" # u"START_DATE ge @lower and START_DATE lt @upper" # dates are in reverse order should be yyyymmdd
  }
}

def get_results(table:str, start_date:int, end_date:int, filters:dict|None={}) -> list[dict]:
    
    stq = SampleTablesQuery(table)    
    
    tconfig = table_config.get(table, {})
    if not tconfig:
      raise Exception("Unknown table name")     
    if not tconfig.get("filter"):
       assessment_commencement_date_limits = None
    else:
       assessment_commencement_date_limits = {u"lower": start_date, u"upper": end_date}
         
    # fields = [u"PartitionKey", u"RowKey", u"Program", u"Staff", u"SurveyName", u"SurveyData"]
    # name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper and IsActive eq 1 and Program ne 'TEST' and Status eq 'Complete'"
    all_filters = tconfig['filter']
    if filters and 'Program' in filters:
      prog_filter_list = [f"Program eq '{f}'" for f in filters['Program']]
      progs_filter_str = f'({  " or ".join(prog_filter_list)  })'
            #  (Program eq 'MURMICE' or Program eq 'EUROPATH' or Program eq 'BEGAPATH')
      all_filters = f"{all_filters} and {progs_filter_str}"


    results = [
         dict(json_data)
         for json_data in 
         stq.query_table(tconfig['fields'], filter_template=all_filters, query_params=assessment_commencement_date_limits)
         ]
    return results
