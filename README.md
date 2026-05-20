# DSM-Function
Fun


az storage blob tag list --account-name sftpdatalakedev01 --container-name email-scanpending --name smoke-test/smoke.txt --auth-mode login -o table



# Upload a tiny test file with a simple name
echo "hello" > /tmp/smoke.txt
az storage blob upload \
  --account-name sftpdatalakedev01 \
  --container-name email-scanpending \
  --name smoke-test/smoke.txt \
  --file /tmp/smoke.txt \
  --auth-mode login

# Wait ~2 minutes for Defender to scan, then check tags
sleep 120

az storage blob tag list \
  --account-name sftpdatalakedev01 \
  --container-name email-scanpending \
  --name smoke-test/smoke.txt \
  --auth-mode login -o table



curl -s "$IDENTITY_ENDPOINT?resource=https://database.windows.net/&api-version=2019-08-01" \
  -H "X-IDENTITY-HEADER: $IDENTITY_HEADER"

SELECT 
    dp.name AS principal_name,
    dp.type_desc,
    dp.authentication_type_desc,
    USER_NAME(rm.role_principal_id) AS role_name
FROM sys.database_principals dp
LEFT JOIN sys.database_role_members rm 
    ON dp.principal_id = rm.member_principal_id
WHERE dp.name = 'func-eus2-dev-dsm-01';


CREATE USER [func-eus2-dev-dsm-01] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [func-eus2-dev-dsm-01];
ALTER ROLE db_datawriter ADD MEMBER [func-eus2-dev-dsm-01];




Server=tcp:sql-east-support-dev-01.database.windows.net,1433;Database=db-east-support-dev-01;Authentication=Active Directory Default;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;


Update BlobServiceStaticTests.cs (xUnit + Moq) to add tests for the new methods in BlobService.cs: MoveAndExtractCleanBlobsAsync, TryProcessZipBlobAsync, TryProcessZipEntryAsync, TryProcessSingleBlobAsync. Cover happy paths, failure paths, and IsEnabled logger branches. Match existing test patterns. Show complete updated file.

,😂

public async Task<List<string>?> MoveAndExtractCleanBlobsAsync(
    IReadOnlyList<BlobScanInfo> blobs,
    string folderPrefix)
{
    BlobContainerClient destContainerClient = _blobServiceClient.GetBlobContainerClient(_processedContainer);
    await destContainerClient.CreateIfNotExistsAsync();
    
    var movedBlobNames = new List<string>();
    var processedBlobPaths = new List<string>();
    
    foreach (BlobScanInfo blob in blobs)
    {
        bool isZip = blob.Item.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase);
        
        bool success = isZip
            ? await TryProcessZipBlobAsync(blob, destContainerClient, folderPrefix, movedBlobNames, processedBlobPaths)
            : await TryProcessSingleBlobAsync(blob, destContainerClient, folderPrefix, movedBlobNames, processedBlobPaths);
        
        if (!success)
        {
            await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
            return null;
        }
    }
    
    return processedBlobPaths;
}

🤗

private async Task<bool> TryProcessZipBlobAsync(
    BlobScanInfo blob,
    BlobContainerClient destContainerClient,
    string folderPrefix,
    List<string> movedBlobNames,
    List<string> processedBlobPaths)
{
    // Move ALL the code from the `if (isZip) { ... }` block here.
    // At the end of success paths: return true;
    // In each catch that previously did "return null;" → return false;
}

private async Task<bool> TryProcessSingleBlobAsync(
    BlobScanInfo blob,
    BlobContainerClient destContainerClient,
    string folderPrefix,
    List<string> movedBlobNames,
    List<string> processedBlobPaths)
{
    // Move ALL the code from the `else { ... }` block here (non-ZIP path).
    // Success: return true;  Failure (catch): return false;
}

😍





private static byte[] ConvertToPdf(byte[] fileContent, string fileName)
{
    bool isPdf = fileName.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase);

    if (isPdf)
    {
        using var ms = new MemoryStream();
        // Use full namespace to avoid ambiguity with Syncfusion
        var outputDocument = new iTextSharp.text.Document();
        var writer = new iTextSharp.text.pdf.PdfCopy(outputDocument, ms);
        outputDocument.Open();

        // REMOVED: iTextSharp.text.pdf.PdfReader.UnethicalReading = true;
        var reader = new iTextSharp.text.pdf.PdfReader(fileContent);

        for (int i = 1; i <= reader.NumberOfPages; i++)
            writer.AddPage(writer.GetImportedPage(reader, i));

        writer.FreeReader(reader);
        reader.Close();
        writer.Close();
        outputDocument.Close();
        ms.Flush();

        return ms.ToArray();
    }

    // Image → PDF using Syncfusion with full namespaces
    using var memoryStream = new MemoryStream(fileContent);
    var pdfDoc = new Syncfusion.Pdf.PdfDocument();
    var pdfPage = pdfDoc.Pages.Add();
    var graphics = pdfPage.Graphics;

    // Load image directly as stream — no FrameCount/ActiveFrame needed
    var pdfImage = new Syncfusion.Pdf.Graphics.PdfBitmap(memoryStream);
    graphics.DrawImage(pdfImage, 0, 0, pdfPage.GetClientSize().Width, pdfPage.GetClientSize().Height);

    using var pdfStream = new MemoryStream();
    pdfDoc.Save(pdfStream);
    pdfDoc.Close(true);

    return pdfStream.ToArray();
}



cd C:\Users\KomalMehetre\source\repos
rmdir /s /q pdp-documentstorage-api
git clone https://dev.azure.com/techcaqhorg/Nexus-Platform-Apps/_git/pdp-documentstorage-api


az functionapp deployment source config-zip `
  --name func-eus2-dsn-dev-06 `
  --resource-group rg-east-datalake-dev-01 `
  --src ./publish.zip



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

  


  
