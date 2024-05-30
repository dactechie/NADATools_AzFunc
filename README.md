
# NADATools_AzFunc
- Automation tools for validation and generation of files for upload to NADAbase

## Validation

# Devlopment setup
* before debuging a function app, run azurite
* assuming azuite is instlled globally on the system rnun

* to run : azurite -s -l c:\azurite -d c:\azurite\debug.log
* see the vscode extension for more details

##  requiremetns.txt development dependency:
* -e C:\\Users\\aftab.jalal\\dev\\assessment_episode_matcher


## Pushing to cloud:
if not using the azure extension then:
func azure functionapp publish nada-tools-directions --build remote