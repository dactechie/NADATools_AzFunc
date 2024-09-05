
# NADATools_AzFunc

- Automation tools for validation and generation of files for upload to NADAbase

## Validation

# Devlopment setup

- before debuging a function app, run azurite
- assuming azuite is instlled globally on the system rnun

- to run : azurite -s -l c:\azurite -d c:\azurite\debug.log
- see the vscode extension for more details

## requiremetns.txt development dependency

- -e C:\\Users\\aftab.jalal\\dev\\assessment_episode_matcher
otherwise 
assessment_episode_matcher==0.6.7

## Pushing to cloud

 Open the VsCode Azure Extn and Expand - Function App> nada-tools-directions-slots
 right click on staging and deploy
 in the context menu that pop-up on the top , select the nada-tools-functions-staging option

if not using the azure extension then:
func azure functionapp publish nada-tools-directions --build remote

## Open telemetry

[OpenTelementry](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry/samples/loggig)
