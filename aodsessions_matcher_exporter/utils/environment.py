import os
from enum import Enum, StrEnum
# from typing import Protocol
# from typing import TypedDict
from dotenv import load_dotenv

# envcondig = TypedDict('EnvironmentConfig', {"access_key": str, "endpoint_suffix": str, "account_name": str, "connection_string": str})

# current_env =   os.getenv("APP_ENVIRONMENT", "prod")
# class EnvironmentConfig(Protocol):
#     env:str
#     connection_string:str
#     survey_table_name:str

class ConfigKeys(StrEnum):
  REFRESH_ATOM_DATA = 'REFRESH_ATOM_DATA'
  TABLES_STORAGE_ENDPOINT_SUFFIX = 'TABLES_STORAGE_ENDPOINT_SUFFIX'
  TABLES_STORAGE_ACCOUNT_NAME = 'TABLES_STORAGE_ACCOUNT_NAME'
  AZURE_STORAGE_CONNECTION_STRING = 'AZURE_STORAGE_CONNECTION_STRING'
  SURVEY_TABLE_NAME =  'SURVEY_TABLE_NAME'
  MATCHING_NDAYS_SLACK = 'MATCHING_NDAYS_SLACK'

class MyEnvironmentConfig:
    _instance = None
    env:str
    # connection_string:str
    # matching_ndays_slack:int
    env_config:dict
    # survey_table_name:str

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MyEnvironmentConfig, cls).__new__(cls)
            # cls._instance.env = 'local'
        return cls._instance
  
    @classmethod
    def setup(cls, env:str):
        cls.env = env
        env_file = f'.env.{cls.env}'
        if not os.path.isfile(env_file):
            raise Exception(f"Environment file {env_file} not found.")
            
        load_dotenv(env_file)
        
        # cls.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "BlankConnectionString")
        # cls.matching_ndays_slack = int(os.getenv("MATCHING_NDAYS_SLACK",0))

        cls.env_config = {key: value for key, value in os.environ.items()}
        
        # cls.survey_table_name = os.getenv("SURVEY_TABLE_NAME","BlankTableName")
    @property
    def config(self):
        return self.env_config        
    # @classmethod
    # def get_new_config_value(cls):
    #     return cls.new_config_value