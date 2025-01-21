# import os
# # from logging import INFO, getLogger
# # See Documentation:
# # https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry/samples/logging

# from azure.monitor.opentelemetry import configure_azure_monitor

# def setup_logger_with_app_insights(logger_name: str) -> None:
#   """Configure logger with Application Insights connection string."""

#   app_insights_connection_string = os.environ.get('APPLICATIONINSIGHTS_CONNECTION_STRING'                                                      
#                                                   ,"")
#   # app_insights_connection_string = os.environ.get(str(AzAppConfig
#   #                                                     .APPLICATIONINSIGHTS_CONNECTION_STRING
#   #                                                     .value)
#   #                                                 ,"")

#   if not app_insights_connection_string:
#     raise Exception("Please set the 'APPLICATIONINSIGHTS_CONNECTION_STRING' environment variable.")

#   configure_azure_monitor(
#       # Set logger_name to the name of the logger you want to capture logging telemetry with
#       logger_name=logger_name,
#       # Set connection_string to your Azure Monitor connection string
#       connection_string=app_insights_connection_string
#   )
