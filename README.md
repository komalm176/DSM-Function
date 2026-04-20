# DSM-Function
Function-logic

cd C:\Users\KomalMehetre\source\repos\DSMTest\LogicApp\email-ingestion-function\EmailUpload.Functions

dotnet publish -c Release

az functionapp deployment source config-zip `
  --name func-eus2-dsn-dev-05 `
  --resource-group rg-eus2-datalake-dev-01 `
  --src bin\Release\net8.0\publish.zip


  Compress-Archive -Path "bin\Release\net8.0\publish\*" -DestinationPath "bin\Release\net8.0\publish.zip" -Force


  
