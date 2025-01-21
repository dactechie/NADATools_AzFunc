
# NADATools_AzFunc

- Automation tools for validation and generation of files for upload to NADAbase

# Data/Code flow

```mermaid
graph TD
    A[Client Request] --> B[perform_mds_atom_matches]
    B --> C[ATOMEpisodeMatcher.run]
    C --> D[io.load_for_period for episodes]
    D --> E[BlobFileSource.load_csv_file_to_df]
    C --> F[episodes.import_data]
    F --> G[episodes.prepare]
    C --> H[io.load_for_period for assessments]
    H --> I[BlobFileSource.load_parquet_file_to_df]
    C --> J[assessments.import_data]
    J --> K[io.get_from_source]
    K --> L[azutil.helper.get_results]
    C --> M[match_helper.get_data_for_matching2]
    M --> N[get_asmts_4_active_eps2]
    C --> O[match_helper.match_and_get_issues]
    O --> P[filter_asmt_by_ep_programs]
    O --> Q[do_matches_slkprog]
    Q --> R[get_merged_for_matching]
    Q --> S[perform_date_matches]
    S --> T[increasing_slack.match_dates_increasing_slack]
    O --> U[do_matches_slk]
    U --> V[get_merged_for_matching]
    U --> W[perform_date_matches]
    O --> X[get_closest_slk_match]
    X --> Y[find_nearest_matches]
```

# Development setup

- before debuging a function app, run azurite
- assuming azuite is instlled globally on the system rnun

- to run : azurite -s -l c:\azurite -d c:\azurite\debug.log
- see the vscode extension for more details

## requiremetns.txt development dependency

- -e C:\\Users\\aftab.jalal\\dev\\assessment_episode_matcher
otherwise 
assessment_episode_matcher==0.6.7

## Pushing to cloud

Staging slot is now deployed vi github actions - just push to main branch

- Old Way: Open the VsCode Azure Extn and Expand - Function App> nada-tools-directions-slots
  right click on staging and deploy
  in the context menu that pop-up on the top , select the nada-tools-functions-staging option

  if not using the azure extension then:
  func azure functionapp publish nada-tools-directions --build remote

  Old way : Pushing to staging SLOT :
  func azure functionapp publish nada-tools-directions --build remote --slot staging

## Open telemetry

[OpenTelementry](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry/samples/logging)


## Git
pushing : git push origin main 
git remote -v 
 has PAT embedded in the url

 not using wincred
 git config --global --unset user.name
  git config --global --unset user.email