
# TO DO

* deploy to cloud
* make runnable as CLI
* pull out into its own package (separate from az func) and make it deployable independently

# IMPORTANT before publishing

https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python?pivots=python-mode-decorators&tabs=asgi%2Capplication-level#programming-model

If your project uses packages that aren't publicly available to our tools, you can make them available to your app by putting them in the __app__/.python_packages directory. Before you publish, run the following command to install the dependencies locally:

pip install  --target="<PROJECT_DIR>/.python_packages/lib/site-packages"  -r requirements.txt

When you're using custom dependencies, you should use the --no-build publishing option, because you've already installed the dependencies into the project folder.

func azure functionapp publish <APP_NAME> --no-build
Remember to replace <APP_NAME> with the name of your function app in Azure.


# Requirements.txt :

azure-core==1.30.0
azure-functions==1.18.0
-e C:\\Users\\aftab.jalal\\dev\\assessment_episode_matcher