using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using EmailUpload.Functions.Helpers;
using EmailUpload.Functions.Models;
using Microsoft.Extensions.Logging;
using Syncfusion.Pdf;
using Syncfusion.Pdf.Graphics;
using iTextSharp.text;
using iTextSharp.text.pdf;

namespace EmailUpload.Functions.Services
{
    /// <summary>
    /// Handles all Azure Blob Storage operations for the email processor.
    /// </summary>
    public class BlobService : IBlobService
    {
        private readonly ILogger<BlobService> _logger;
        private readonly BlobServiceClient    _blobServiceClient;
        private readonly int                  _maxMoveRetries;

        // Container names
        private readonly string _processedContainer;
        private readonly string _quarantineContainer;
        private readonly string _scanPendingContainer;

        public BlobService(ILogger<BlobService> logger, BlobServiceClient blobServiceClient)
        {
            _logger            = logger;
            _blobServiceClient = blobServiceClient;
            _maxMoveRetries    = int.TryParse(Environment.GetEnvironmentVariable("MaxMoveRetries"), out int m) ? m : 3;

            _processedContainer   = Environment.GetEnvironmentVariable("ProcessedContainer")   ?? "email-processed";
            _quarantineContainer  = Environment.GetEnvironmentVariable("QuarantineContainer")  ?? "email-quarantine";
            _scanPendingContainer = Environment.GetEnvironmentVariable("ScanPendingContainer") ?? "email-scanpending";
        }

        // ---------------------------------------------------------------
        // Container names (read-only for function)
        // ---------------------------------------------------------------

        public string ProcessedContainer    => _processedContainer;
        public string QuarantineContainer   => _quarantineContainer;
        public string ScanPendingContainer  => _scanPendingContainer;

        // ---------------------------------------------------------------
        // Blob listing
        // ---------------------------------------------------------------

        /// <summary>
        /// Checks if a staging container exists.
        /// </summary>
        public async Task<bool> ContainerExistsAsync(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
            return await containerClient.ExistsAsync();
        }

        /// <summary>
        /// Lists all blobs under the given folder prefix, reading scan metadata.
        /// </summary>
        public async Task<List<BlobScanInfo>> GetBlobsFromFolderAsync(
            string containerName,
            string folderPrefix)
        {
            var blobs = new List<BlobScanInfo>();
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerName);

            await foreach (BlobItem blobItem in containerClient.GetBlobsAsync(
                traits: BlobTraits.Metadata, states: BlobStates.None, prefix: folderPrefix, cancellationToken: default))
            {
                BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);
                blobItem.Metadata.TryGetValue("ScanResult", out string? scanResult);
                blobItem.Metadata.TryGetValue("RetryCount", out string? retryStr);
                int retryCount = int.TryParse(retryStr, out int r) ? r : 0;

                blobs.Add(new BlobScanInfo(blobClient, blobItem, scanResult?.Trim(), retryCount));
            }

            return blobs;
        }

        // ---------------------------------------------------------------
        // Recovery check
        // ---------------------------------------------------------------

        /// <summary>
        /// Checks destination containers for blobs with the given folder prefix.
        /// Used to recover from a previous run where blobs were moved but DB insert failed.
        /// Returns the destination folder path and inferred status, or null if not found.
        /// </summary>
        public async Task<(string DestFolder, string Status)?> FindBlobsInDestinationAsync(string folderPrefix)
        {
            var destinations = new[]
            {
                (_processedContainer,   EmailStatus.ReadyForUpload),
                (_quarantineContainer,  EmailStatus.Malicious),
                (_scanPendingContainer, EmailStatus.ScanPending),
            };

            foreach (var (container, status) in destinations)
            {
                BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(container);
                bool found = false;

                await foreach (BlobItem blobItem in containerClient.GetBlobsAsync(
                    traits: BlobTraits.Metadata, states: BlobStates.None, prefix: folderPrefix, cancellationToken: default))
                {
                    if (blobItem.Metadata.TryGetValue("DestinationContainer", out string? dest)
                        && dest == container)
                    {
                        found = true;
                        break;
                    }
                }

                if (found)
                    return ($"{container}/{folderPrefix}", status);
            }

            return null;
        }

        // ---------------------------------------------------------------
        // Retry count increment
        // ---------------------------------------------------------------

        /// <summary>
        /// Increments RetryCount in blob metadata for each still-scanning blob.
        /// Retries the metadata write up to MaxMoveRetries times.
        /// Returns false if any metadata update fails after all retries.
        /// </summary>
        public async Task<bool> IncrementRetryCountAsync(
            IEnumerable<BlobScanInfo> scanningBlobs,
            string                    messageId)
        {
            foreach (BlobScanInfo blob in scanningBlobs)
            {
                bool updated = false;

                for (int attempt = 1; attempt <= _maxMoveRetries; attempt++)
                {
                    try
                    {
                        var updatedMetadata = new Dictionary<string, string>(blob.Item.Metadata)
                        {
                            ["RetryCount"] = (blob.RetryCount + 1).ToString()
                        };
                        await blob.Client.SetMetadataAsync(updatedMetadata);

                        _logger.LogInformation(
                            "RetryCount incremented to {Count} for blob {Name}, MessageId={Id}.",
                            blob.RetryCount + 1, blob.Item.Name, messageId);

                        updated = true;
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex,
                            "Metadata update attempt {Attempt}/{Max} failed for {Name}.",
                            attempt, _maxMoveRetries, blob.Item.Name);

                        if (attempt < _maxMoveRetries)
                            await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
                    }
                }

                if (!updated)
                {
                    _logger.LogCritical(
                        "Failed to update RetryCount for blob {Name} after {Max} attempts. MessageId={Id}.",
                        blob.Item.Name, _maxMoveRetries, messageId);
                    return false;
                }
            }

            return true;
        }

        // ---------------------------------------------------------------
        // Move all blobs
        // ---------------------------------------------------------------

        /// <summary>
        /// Moves ALL blobs to the destination container preserving folder structure.
        /// Retries each blob up to MaxMoveRetries times.
        /// Rolls back on any failure.
        /// Returns destination folder path or null on failure.
        /// </summary>
        public async Task<string?> MoveAllBlobsWithRollbackAsync(
            IReadOnlyList<BlobScanInfo> blobs,
            string                      destContainer,
            string                      folderPrefix)
        {
            BlobContainerClient destContainerClient = _blobServiceClient.GetBlobContainerClient(destContainer);
            await destContainerClient.CreateIfNotExistsAsync();

            var movedBlobNames = new List<string>();

            foreach (BlobScanInfo blob in blobs)
            {
                bool moved = await TryMoveBlobWithRetryAsync(
                    blob.Client, destContainerClient, blob.Item.Name, destContainer);

                if (!moved)
                {
                    _logger.LogCritical(
                        "Failed to move blob {Name} to {Container} after {Max} retries. Rolling back.",
                        blob.Item.Name, destContainer, _maxMoveRetries);

                    await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                    return null;
                }

                await SetDestinationMetadataAsync(destContainerClient, blob.Item.Name, destContainer);
                movedBlobNames.Add(blob.Item.Name);
            }

            return $"{destContainer}/{folderPrefix}";
        }

        // ---------------------------------------------------------------
        // Move and extract clean blobs  (UPDATED: convert to PDF + return per-file paths)
        // ---------------------------------------------------------------

        /// <summary>
        /// Moves clean blobs to the processed container.
        /// ZIP files are extracted into the same destination folder.
        /// All files are converted to PDF before upload.
        /// Handles duplicate filenames by appending -1, -2, etc.
        /// Rolls back on any failure.
        /// Returns list of individual processed blob paths (one per file), or null on failure.
        /// </summary>
        public async Task<List<string>?> MoveAndExtractCleanBlobsAsync(
            IReadOnlyList<BlobScanInfo> blobs,
            string                      folderPrefix)
        {
            BlobContainerClient destContainerClient =
                _blobServiceClient.GetBlobContainerClient(_processedContainer);
            await destContainerClient.CreateIfNotExistsAsync();

            var movedBlobNames     = new List<string>(); // for rollback
            var processedBlobPaths = new List<string>(); // one per file — returned to caller

            foreach (BlobScanInfo blob in blobs)
            {
                bool isZip = blob.Item.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase);

                if (isZip)
                {
                    _logger.LogInformation("ZIP detected – extracting: {Name}", blob.Item.Name);

                    // Download zip
                    using var ms = new MemoryStream();
                    try
                    {
                        await blob.Client.DownloadToAsync(ms);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogCritical(ex, "Failed to download ZIP blob {Name}.", blob.Item.Name);
                        await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                        return null;
                    }

                    ms.Position = 0;

                    try
                    {
                        using var archive = new ZipArchive(ms, ZipArchiveMode.Read);
                        foreach (ZipArchiveEntry entry in archive.Entries)
                        {
                            if (string.IsNullOrEmpty(entry.Name)) continue;

                            // Read entry bytes
                            byte[] fileBytes;
                            using (var entryStream = entry.Open())
                            using (var entryMs = new MemoryStream())
                            {
                                await entryStream.CopyToAsync(entryMs);
                                fileBytes = entryMs.ToArray();
                            }

                            // Convert to PDF
                            byte[] pdfBytes;
                            try
                            {
                                pdfBytes = ConvertToPdf(fileBytes, entry.Name);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogCritical(ex,
                                    "Failed to convert ZIP entry {Entry} to PDF.", entry.Name);
                                await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                                return null;
                            }

                            // Unique PDF blob name
                            string pdfEntryName   = Path.GetFileNameWithoutExtension(entry.Name) + ".pdf";
                            string uniqueBlobName  = await GetUniqueBlobNameAsync(
                                destContainerClient, folderPrefix, pdfEntryName);

                            // Upload PDF
                            BlobClient destBlob = destContainerClient.GetBlobClient(uniqueBlobName);
                            using var pdfUploadStream = new MemoryStream(pdfBytes);
                            await destBlob.UploadAsync(pdfUploadStream, overwrite: false);

                            await SetDestinationMetadataAsync(
                                destContainerClient, uniqueBlobName, _processedContainer);

                            movedBlobNames.Add(uniqueBlobName);
                            processedBlobPaths.Add($"{_processedContainer}/{uniqueBlobName}");

                            _logger.LogInformation(
                                "Extracted+converted {Entry} → {Container}/{BlobName}",
                                entry.Name, _processedContainer, uniqueBlobName);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogCritical(ex, "Failed to extract ZIP blob {Name}.", blob.Item.Name);
                        await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                        return null;
                    }

                    // Delete original zip from staging
                    await blob.Client.DeleteIfExistsAsync();
                }
                else
                {
                    string fileName = Path.GetFileName(blob.Item.Name);

                    // Download
                    byte[] fileBytes;
                    using (var fileMs = new MemoryStream())
                    {
                        try
                        {
                            await blob.Client.DownloadToAsync(fileMs);
                            fileBytes = fileMs.ToArray();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogCritical(ex, "Failed to download blob {Name}.", blob.Item.Name);
                            await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                            return null;
                        }
                    }

                    // Convert to PDF
                    byte[] pdfBytes;
                    try
                    {
                        pdfBytes = ConvertToPdf(fileBytes, fileName);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogCritical(ex,
                            "Failed to convert blob {Name} to PDF.", blob.Item.Name);
                        await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                        return null;
                    }

                    // Unique PDF name
                    string pdfFileName = Path.GetFileNameWithoutExtension(fileName) + ".pdf";
                    string uniqueName  = await GetUniqueBlobNameAsync(
                        destContainerClient, folderPrefix, pdfFileName);

                    // Upload PDF
                    BlobClient destBlob = destContainerClient.GetBlobClient(uniqueName);
                    using var pdfStream = new MemoryStream(pdfBytes);
                    await destBlob.UploadAsync(pdfStream, overwrite: false);

                    await SetDestinationMetadataAsync(
                        destContainerClient, uniqueName, _processedContainer);

                    movedBlobNames.Add(uniqueName);
                    processedBlobPaths.Add($"{_processedContainer}/{uniqueName}");

                    // Delete original from staging
                    await blob.Client.DeleteIfExistsAsync();

                    _logger.LogInformation(
                        "Converted+moved {Name} → {Container}/{Dest}",
                        blob.Item.Name, _processedContainer, uniqueName);
                }
            }

            return processedBlobPaths;
        }

        // ---------------------------------------------------------------
        // Delete staging folder
        // ---------------------------------------------------------------

        /// <summary>
        /// Deletes all blobs under the staging folder prefix after successful move.
        /// </summary>
        public async Task DeleteStagingFolderAsync(string containerName, string folderPrefix)
        {
            try
            {
                BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                await foreach (BlobItem blobItem in containerClient.GetBlobsAsync(
                    traits: BlobTraits.None, states: BlobStates.None, prefix: folderPrefix, cancellationToken: default))
                {
                    await containerClient.GetBlobClient(blobItem.Name).DeleteIfExistsAsync();
                    _logger.LogInformation("Deleted staging blob: {Name}", blobItem.Name);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to delete staging folder {Prefix}. Manual cleanup may be needed.", folderPrefix);
            }
        }

        // ---------------------------------------------------------------
        // Private helpers
        // ---------------------------------------------------------------

        /// <summary>
        /// Copies blob to destination and deletes source.
        /// Retries up to MaxMoveRetries with exponential backoff.
        /// Returns true on success.
        /// </summary>
        private async Task<bool> TryMoveBlobWithRetryAsync(
            BlobClient            source,
            BlobContainerClient   destContainer,
            string                destBlobName,
            string                destContainerName)
        {
            for (int attempt = 1; attempt <= _maxMoveRetries; attempt++)
            {
                try
                {
                    BlobClient dest   = destContainer.GetBlobClient(destBlobName);
                    var        copyOp = await dest.StartCopyFromUriAsync(source.Uri);
                    await copyOp.WaitForCompletionAsync();
                    await source.DeleteIfExistsAsync();

                    _logger.LogInformation(
                        "Moved {Name} → {Container}/{Dest}",
                        source.Name, destContainerName, destBlobName);

                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "Move attempt {Attempt}/{Max} failed for blob {Name}.",
                        attempt, _maxMoveRetries, source.Name);

                    if (attempt < _maxMoveRetries)
                        await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
                }
            }

            return false;
        }

        /// <summary>
        /// Deletes all blobs from destination that were moved in current operation.
        /// Called during rollback.
        /// </summary>
        private async Task RollbackMovedBlobsAsync(
            BlobContainerClient      destContainer,
            IEnumerable<string>      movedBlobNames)
        {
            foreach (string blobName in movedBlobNames)
            {
                try
                {
                    await destContainer.GetBlobClient(blobName).DeleteIfExistsAsync();
                    _logger.LogInformation(
                        "Rollback: deleted {Name} from {Container}.",
                        blobName, destContainer.Name);
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(ex,
                        "Rollback: FAILED to delete {Name} from {Container}.",
                        blobName, destContainer.Name);
                }
            }
        }

        /// <summary>
        /// Sets DestinationContainer metadata on a moved blob for recovery purposes.
        /// </summary>
        private async Task SetDestinationMetadataAsync(
            BlobContainerClient destContainer,
            string              blobName,
            string              destContainerName)
        {
            try
            {
                BlobClient blobClient = destContainer.GetBlobClient(blobName);
                var        props      = await blobClient.GetPropertiesAsync();
                var        metadata   = new Dictionary<string, string>(props.Value.Metadata)
                {
                    ["DestinationContainer"] = destContainerName
                };
                await blobClient.SetMetadataAsync(metadata);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to set DestinationContainer metadata on blob {Name}.", blobName);
            }
        }

        /// <summary>
        /// Returns a unique blob name — appends -1, -2, etc. if name already exists.
        /// </summary>
        private static async Task<string> GetUniqueBlobNameAsync(
            BlobContainerClient container,
            string              folderPrefix,
            string              fileName)
        {
            string nameNoExt  = Path.GetFileNameWithoutExtension(fileName);
            string ext        = Path.GetExtension(fileName);
            string candidate  = $"{folderPrefix}{fileName}";
            int    counter    = 1;

            while (await container.GetBlobClient(candidate).ExistsAsync())
            {
                candidate = $"{folderPrefix}{nameNoExt}-{counter}{ext}";
                counter++;
            }

            return candidate;
        }

        // ---------------------------------------------------------------
        // Static utility
        // ---------------------------------------------------------------

        /// <summary>
        /// Parses "email-staging/MSG001_20240418120000/" into
        /// container = "email-staging" and folderPrefix = "MSG001_20240418120000/".
        /// </summary>
        public static (string Container, string FolderPrefix) ParseContainerAndPrefix(string folderPath)
        {
            int slash = folderPath.IndexOf('/');
            if (slash < 0)
                throw new ArgumentException(
                    $"Invalid folder path — missing container separator: {folderPath}");

            string container    = folderPath[..slash];
            string folderPrefix = folderPath[(slash + 1)..];

            if (!folderPrefix.EndsWith('/'))
                folderPrefix += '/';

            return (container, folderPrefix);
        }

        // ---------------------------------------------------------------
        // PDF Conversion helper
        // ---------------------------------------------------------------

        /// <summary>
        /// Converts file bytes to a PDF byte array.
        /// - PDF files    : repaired/re-serialised via iTextSharp PdfCopy
        /// - Image files  : converted via Syncfusion PdfBitmap (matches PDP logic)
        /// </summary>
        private static byte[] ConvertToPdf(byte[] fileContent, string fileName)
        {
            bool isPdf = fileName.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase);

            if (isPdf)
            {
                // Repair/re-serialise using iTextSharp (PDP fallback pattern)
                using var ms             = new MemoryStream();
                var       outputDocument = new iTextSharp.text.Document();
                var       writer         = new iTextSharp.text.pdf.PdfCopy(outputDocument, ms);
                outputDocument.Open();

                iTextSharp.text.pdf.PdfReader.UnethicalReading = true;
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

            // Image → PDF via Syncfusion (matches PDP ConvertImageToPdf exactly)
            using var memoryStream = new MemoryStream(fileContent);
            var pdfDoc    = new PdfDocument();
            var pdfBitmap = new PdfBitmap(memoryStream);
            int frameCount = pdfBitmap.FrameCount;

            for (int i = 0; i < frameCount; i++)
            {
                PdfPage     page = pdfDoc.Pages.Add();
                PdfGraphics g    = page.Graphics;
                pdfBitmap.ActiveFrame = i;
                g.DrawImage(pdfBitmap, 0, 0, 500, 500);
            }

            using var pdfStream = new MemoryStream();
            pdfDoc.Save(pdfStream);
            pdfDoc.Close(true);

            return pdfStream.ToArray();
        }
    }
}
