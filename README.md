# DSM-Function
Function-logicapp


az network private-endpoint list `
  --resource-group rg-east-datalake-dev-01 `
  --query "[?contains(name, 'func')]" `
  --output table


az functionapp deploy `
  --name func-eus2-dsn-dev-06 `
  --resource-group rg-east-datalake-dev-01 `
  --src-path ./publish.zip `
  --type zip




az resource update `
  --resource-group rg-east-datalake-dev-01 `
  --name func-eus2-dsn-dev-06/basicPublishingCredentialsPolicies/scm `
  --resource-type "Microsoft.Web/sites/basicPublishingCredentialsPolicies" `
  --set properties.allow=true



# Check basic auth status for SCM
az resource show `
  --resource-group rg-east-datalake-dev-01 `
  --name func-eus2-dsn-dev-06/basicPublishingCredentialsPolicies/scm `
  --resource-type "Microsoft.Web/sites/basicPublishingCredentialsPolicies"




# Check if basic auth is enabled for SCM
az functionapp config access-restriction show `
  --name func-eus2-dsn-dev-06 `
  --resource-group rg-east-datalake-dev-01



az resource update `
  --resource-group rg-east-datalake-dev-01 `
  --name func-eus2-dsn-dev-06 `
  --resource-type "Microsoft.Web/sites" `
  --set properties.basicPublishingCredentialsPolicies.scm.allow=true


az functionapp deployment source config-zip `
  --name func-eus2-dsn-dev-06 `
  --resource-group rg-east-datalake-dev-01 `
  --src ./publish.zip `
  --build-remote true






{
  "MessageId": "TEST001-outlook-com",
  "SenderAddress": "test@company.com",
  "EmailSubject": "Test Email",
  "EmailBody": "Test body",
  "EmailAttachmentPath": null,
  "EmailStatus": null,
  "EmailStatusReason": null,
  "SendDateTime": "2024-04-18T12:00:00Z",
  "CreatedDateTime": null
}






"inputs": "@coalesce(item()?['from']?['emailAddress']?['address'], 'unknown@unknown.com')",





{
  "type": "Compose",
  "inputs": {
    "messageId": "@outputs('Sanitise_MessageId')",
    "emailSubject": "@item()?['Subject']",
    "emailBody": "@item()?['Body']",
    "senderAddress": "@outputs('Extract_Sender_Address')",
    "receivedDateTime": "@item()?['receivedDateTime']",
    "blobFolderPath": "@outputs('Build_Folder_Path')"
  }
}







@base64(string(json(concat(
  '{"messageId":"', outputs('Sanitise_MessageId'),
  '","emailSubject":"', item()?['Subject'],
  '","emailBody":"', item()?['Body'],
  '","senderAddress":"', outputs('Extract_Sender_Address'),
  '","receivedDateTime":"', item()?['receivedDateTime'],
  '","blobFolderPath":"', outputs('Build_Folder_Path'),
  '"}'
))))



{
  "MessageId": "TEST001-outlook-com",
  "SenderAddress": "test@company.com",
  "EmailSubject": "Test Email",
  "EmailBody": "Test body",
  "EmailAttachmentPath": null,
  "EmailStatus": null,
  "EmailStatusReason": null,
  "SendDateTime": "2024-04-18T12:00:00Z",
  "CreatedDateTime": null
}




{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage"                   : "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME"              : "dotnet-isolated",
    "ServiceBusConnection"                  : "<YOUR-SERVICE-BUS-CONNECTION-STRING>",
    "ServiceBusQueueName"                   : "email-ingestion-queue",
    "SqlConnectionString"                   : "Server=(localdb)\\mssqllocaldb;Database=EmailProcessor;Integrated Security=True;TrustServerCertificate=True;",
    "MaxScanRetries"                        : "3",
    "MaxMoveRetries"                        : "3",
    "LockRenewalIntervalSeconds"            : "30",
    "ProcessedContainer"                    : "email-processed",
    "QuarantineContainer"                   : "email-quarantine",
    "ScanPendingContainer"                  : "email-scanpending",
    "StagingContainer"                      : "email-staging",
    "APPLICATIONINSIGHTS_CONNECTION_STRING" : "<YOUR-APP-INSIGHTS-CONNECTION-STRING>"
  }
}



"SqlConnectionString": "Server=(localdb)\\mssqllocaldb;Database=EmailProcessor;Integrated Security=True;TrustServerCertificate=True;"



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

  


  
