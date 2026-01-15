# NADATools_AzFunc Development Guide

## Setup & Commands
- Start Azurite: `azurite -s -l c:\azurite -d c:\azurite\debug.log`
- Local Development: Run VS Code with Azure Functions extension
- Deploy: Push to main branch (GitHub Actions deploys to staging slot)
- Manual deploy: `func azure functionapp publish nada-tools-directions --build remote`

## Testing
- No standardized test suite yet - local testing via Azure Functions emulator
- Debug with VS Code launch settings
- Verify matching logic with real data samples in data/in directories

## Code Style Guidelines
- Imports: standard lib first, then 3rd party packages, then local modules
- Types: Use typing annotations (see Optional[list[str]] usage)
- Error handling: Use try/except with specific exceptions
- Logging: Use logging.info/error/warning, not print statements
- Naming: snake_case for functions/variables, PascalCase for classes
- Function structure: Accept explicit parameters, return well-defined outputs
- Config: Use environment variables and configuration.json from blob storage

## Dependencies
- azure-functions
- azure-storage-blob
- assessment_episode_matcher (local development package)