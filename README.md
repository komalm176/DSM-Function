# DSM-Function
Function-logic


set PATH=%PATH%;C:\Users\KomalMehetre\.azurelogicapps\dependencies\FuncCoreTools\in-proc8

cd C:\Users\KomalMehetre\source\repos\DSMTest\LogicApp\email-ingestion-function\EmailUpload.Functions.Refactored\EmailUpload.Functions

func start


$env:PATH += ";C:\Users\KomalMehetre\.azurelogicapps\dependencies\FuncCoreTools\in-proc8"



& "C:\Users\KomalMehetre\.azurelogicapps\dependencies\FuncCoreTools\in-proc8\func.exe" start


[System.Environment]::SetEnvironmentVariable(
  "PATH",
  [System.Environment]::GetEnvironmentVariable("PATH","User") + ";C:\Users\KomalMehetre\.azurelogicapps\dependencies\FuncCoreTools\in-proc8",
  [System.EnvironmentVariableTarget]::User
)




$env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")




cd C:\Users\KomalMehetre\source\repos\DSMTest\LogicApp\email-ingestion-function\EmailUpload.Functions

dotnet publish -c Release

az functionapp deployment source config-zip `
  --name func-eus2-dsn-dev-05 `
  --resource-group rg-eus2-datalake-dev-01 `
  --src bin\Release\net8.0\publish.zip


  Compress-Archive -Path "bin\Release\net8.0\publish\*" -DestinationPath "bin\Release\net8.0\publish.zip" -Force




  az functionapp deployment source config-zip `
  --name func-eus2-dsn-dev-05 `
  --resource-group rg-eus2-datalake-dev-01 `
  --subscription c7fd7250-bb98-4ce0-bf7e-deba223c1151 `
  --src "bin\Release\net8.0\publish.zip"

  


  
