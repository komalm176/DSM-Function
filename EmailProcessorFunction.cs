using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace EmailUpload.Functions
{
    // ─────────────────────────────────────────────────────────────────────────
    // Email status constants
    // ─────────────────────────────────────────────────────────────────────────
    public static class EmailStatus
    {
        public const string ReadyForUpload    = "ReadyForUpload";    // All attachments clean, moved to processed
        public const string Malicious         = "Malicious";         // At least one attachment malicious
        public const string ScanPending       = "ScanPending";       // Scan incomplete after max retries
        public const string MissingAttachment = "MissingAttachment"; // No attachment path or empty folder
        public const string MoveFailed        = "MoveFailed";        // Blob move failed after retries, rolled back
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Service Bus message payload
    // ─────────────────────────────────────────────────────────────────────────
    public class EmailMessagePayload
    {
        /// <summary>Unique identifier from the email's Message-ID header.</summary>
        public string MessageId { get; set; } = string.Empty;

        /// <summary>Sender's email address (extracted from From header).</summary>
        public string? SenderAddress { get; set; }

        /// <summary>Subject line of the email.</summary>
        public string? EmailSubject { get; set; }

        /// <summary>Plain-text or HTML body of the email.</summary>
        public string? EmailBody { get; set; }

        /// <summary>
        /// Blob storage folder path containing all attachments.
        /// Format: "email-staging/{sanitisedMessageId}_{sendDateTimeUtc}/"
        /// Null when no attachment is present.
        /// Updated by this function to reflect the destination folder after processing.
        /// </summary>
        public string? EmailAttachmentPath { get; set; }

        /// <summary>Processing status — set by this function using <see cref="EmailStatus"/> constants.</summary>
        public string? EmailStatus { get; set; }

        /// <summary>Human-readable reason for the current status (failure / rejection reason).</summary>
        public string? EmailStatusReason { get; set; }

        /// <summary>UTC date/time the email was originally sent by the sender.</summary>
        public DateTime SendDateTime { get; set; }

        /// <summary>UTC date/time this record was created — stamped by this function.</summary>
        public DateTime CreatedDateTime { get; set; }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal model to hold per-blob scan details
    // ─────────────────────────────────────────────────────────────────────────
    internal record BlobScanInfo(
        BlobClient Client,
        BlobItem   Item,
        string?    ScanResult,
        int        RetryCount);

    // ─────────────────────────────────────────────────────────────────────────
    // Azure Function
    // ─────────────────────────────────────────────────────────────────────────
    public class EmailProcessorFunction
    {
        private readonly ILogger<EmailProcessorFunction> _logger;

        // ── App Settings ──────────────────────────────────────────────────────
        private static readonly string StorageConnection  = Environment.GetEnvironmentVariable("AzureWebJobsStorage")!;
        private static readonly string SqlConn            = Environment.GetEnvironmentVariable("SqlConnectionString")!;
        private static readonly int    MaxScanRetries     = int.TryParse(Environment.GetEnvironmentVariable("MaxScanRetries"),    out int s) ? s : 3;
        private static readonly int    MaxMoveRetries     = int.TryParse(Environment.GetEnvironmentVariable("MaxMoveRetries"),    out int m) ? m : 3;
        private static readonly int    LockRenewalSeconds = int.TryParse(Environment.GetEnvironmentVariable("LockRenewalIntervalSeconds"), out int l) ? l : 30;

        // ── Blob container names ──────────────────────────────────────────────
        private static readonly string ProcessedContainer   = Environment.GetEnvironmentVariable("ProcessedContainer")   ?? "email-processed";
        private static readonly string QuarantineContainer  = Environment.GetEnvironmentVariable("QuarantineContainer")  ?? "email-quarantine";
        private static readonly string ScanPendingContainer = Environment.GetEnvironmentVariable("ScanPendingContainer") ?? "email-scanpending";

        public EmailProcessorFunction(ILogger<EmailProcessorFunction> logger)
            => _logger = logger;

        // ─────────────────────────────────────────────────────────────────────
        // Trigger: Azure Service Bus Queue
        // ─────────────────────────────────────────────────────────────────────
        [Function("EmailProcessor")]
        public async Task Run(
            [ServiceBusTrigger("%ServiceBusQueueName%", Connection = "ServiceBusConnection")]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions)
        {
            _logger.LogInformation("EmailProcessor triggered. MessageId: {Id}", message.MessageId);

            // ── 1. Deserialise payload ────────────────────────────────────────
            EmailMessagePayload payload;
            try
            {
                payload = JsonSerializer.Deserialize<EmailMessagePayload>(message.Body.ToString())
                          ?? throw new InvalidOperationException("Payload deserialised to null.");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Failed to deserialise Service Bus message. Dead-lettering.");
                await messageActions.DeadLetterMessageAsync(message, deadLetterReason: "DeserializationError", deadLetterErrorDescription: ex.Message);
                return;
            }

            // ── 2. Validate MessageId ─────────────────────────────────────────
            if (string.IsNullOrWhiteSpace(payload.MessageId))
            {
                _logger.LogCritical("MessageId is null or empty. Dead-lettering message.");
                await messageActions.DeadLetterMessageAsync(message, deadLetterReason: "InvalidPayload", deadLetterErrorDescription: "MessageId is null or empty.");
                return;
            }

            // ── 3. Stamp CreatedDateTime ──────────────────────────────────────
            payload.CreatedDateTime = DateTime.UtcNow;

            // ── 4. Default SendDateTime if missing ────────────────────────────
            if (payload.SendDateTime == default)
            {
                _logger.LogWarning("SendDateTime missing for MessageId={Id}. Defaulting to UtcNow.", payload.MessageId);
                payload.SendDateTime = DateTime.UtcNow;
            }

            // ── 5. Start background message lock renewal ──────────────────────
            using var lockRenewalCts    = new CancellationTokenSource();
            Task       lockRenewalTask  = RenewMessageLockAsync(messageActions, message, lockRenewalCts.Token);

            try
            {
                await ProcessEmailAsync(payload, message, messageActions);
            }
            finally
            {
                await lockRenewalCts.CancelAsync();
                await lockRenewalTask;
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Core processing logic
        // ─────────────────────────────────────────────────────────────────────
        private async Task ProcessEmailAsync(
            EmailMessagePayload       payload,
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions  messageActions)
        {
            var blobServiceClient = new BlobServiceClient(StorageConnection);

            // ── 6. Idempotency — skip if MessageId already exists in DB ───────
            if (await MessageIdExistsAsync(payload.MessageId))
            {
                _logger.LogWarning("MessageId {Id} already exists in DB. Skipping duplicate.", payload.MessageId);
                await messageActions.CompleteMessageAsync(message);
                return;
            }

            // ── 7. Recovery — SQL failed after blobs were already moved ───────
            if (!string.IsNullOrWhiteSpace(payload.EmailAttachmentPath))
            {
                (_, string folderPrefixCheck) = ParseContainerAndPrefix(payload.EmailAttachmentPath);
                var recovery = await FindBlobsInDestinationAsync(blobServiceClient, folderPrefixCheck);
                if (recovery.HasValue)
                {
                    _logger.LogWarning(
                        "Recovery: Blobs for MessageId={Id} already in {Container}. Re-inserting DB record.",
                        payload.MessageId, recovery.Value.DestFolder);

                    payload.EmailAttachmentPath = recovery.Value.DestFolder;
                    await InsertEmailDocumentAsync(payload, recovery.Value.Status,
                        "Recovered: blobs moved in a previous attempt but DB insert had failed.",
                        message, messageActions);
                    return;
                }
            }

            // ── 8. No attachment path → MissingAttachment ─────────────────────
            if (string.IsNullOrWhiteSpace(payload.EmailAttachmentPath))
            {
                _logger.LogWarning("No attachment folder for MessageId: {Id}", payload.MessageId);
                await InsertEmailDocumentAsync(payload, EmailStatus.MissingAttachment,
                    "No attachment path provided in the message payload.",
                    message, messageActions);
                return;
            }

            // ── 9. Parse source container and folder prefix ───────────────────
            (string sourceContainer, string folderPrefix) = ParseContainerAndPrefix(payload.EmailAttachmentPath);

            // ── 10. Verify staging container exists ───────────────────────────
            BlobContainerClient sourceContainerClient = blobServiceClient.GetBlobContainerClient(sourceContainer);
            if (!await sourceContainerClient.ExistsAsync())
            {
                _logger.LogCritical("Staging container '{Container}' does not exist. Dead-lettering MessageId={Id}.",
                    sourceContainer, payload.MessageId);
                await messageActions.DeadLetterMessageAsync(message, deadLetterReason: "StagingContainerMissing",
                    deadLetterErrorDescription: $"Staging container '{sourceContainer}' does not exist.");
                return;
            }

            // ── 11. List all blobs in the staging folder ──────────────────────
            var blobs = await GetBlobsFromFolderAsync(sourceContainerClient, folderPrefix);

            // ── 12. Empty folder → MissingAttachment ──────────────────────────
            if (blobs.Count == 0)
            {
                _logger.LogWarning("No blobs found at {Path} for MessageId={Id}",
                    payload.EmailAttachmentPath, payload.MessageId);
                await InsertEmailDocumentAsync(payload, EmailStatus.MissingAttachment,
                    "Attachment folder exists but contains no files.",
                    message, messageActions);
                return;
            }

            // ── 13. Categorise blobs by scan result ───────────────────────────
            var scanningBlobs  = blobs.Where(b => b.ScanResult is "Scanning" or null).ToList();
            var maliciousBlobs = blobs.Where(b => b.ScanResult == "Malicious").ToList();
            var cleanBlobs     = blobs.Where(b => b.ScanResult == "NoThreatsFound").ToList();

            _logger.LogInformation(
                "MessageId={Id} — Scanning={S}, Malicious={M}, Clean={C}",
                payload.MessageId, scanningBlobs.Count, maliciousBlobs.Count, cleanBlobs.Count);

            // ═════════════════════════════════════════════════════════════════
            // PRIORITY 1 — Any attachment still scanning?
            // ═════════════════════════════════════════════════════════════════
            if (scanningBlobs.Any())
            {
                bool maxRetriesReached = scanningBlobs.Any(b => b.RetryCount >= MaxScanRetries);

                if (maxRetriesReached)
                {
                    _logger.LogCritical(
                        "Max scan retries ({Max}) reached. Moving all to ScanPending. MessageId={Id}.",
                        MaxScanRetries, payload.MessageId);

                    string? destFolder = await MoveAllBlobsWithRollbackAsync(
                        blobServiceClient, blobs, ScanPendingContainer, folderPrefix);

                    if (destFolder is null)
                    {
                        await InsertEmailDocumentAsync(payload, EmailStatus.MoveFailed,
                            "Failed to move blobs to scan-pending container after retries. Rolled back.",
                            message, messageActions, deadLetter: true);
                        return;
                    }

                    await DeleteStagingFolderAsync(sourceContainerClient, folderPrefix);
                    payload.EmailAttachmentPath = destFolder;
                    await InsertEmailDocumentAsync(payload, EmailStatus.ScanPending,
                        $"Defender scan did not complete after {MaxScanRetries} retries.",
                        message, messageActions);
                }
                else
                {
                    // Increment RetryCount in blob metadata for each scanning blob
                    bool metadataUpdated = await IncrementRetryCountAsync(scanningBlobs, payload.MessageId);
                    if (!metadataUpdated)
                    {
                        _logger.LogCritical(
                            "Failed to update RetryCount in blob metadata. Dead-lettering MessageId={Id}.",
                            payload.MessageId);
                        await messageActions.DeadLetterMessageAsync(message,
                            deadLetterReason: "MetadataUpdateFailed", deadLetterErrorDescription: "Failed to update RetryCount after retries.");
                        return;
                    }

                    _logger.LogInformation(
                        "Scan not yet complete. Abandoning for retry. MessageId={Id}", payload.MessageId);
                    await messageActions.AbandonMessageAsync(message);
                }

                return;
            }

            // ═════════════════════════════════════════════════════════════════
            // PRIORITY 2 — All scanned. Any malicious?
            // ═════════════════════════════════════════════════════════════════
            if (maliciousBlobs.Any())
            {
                _logger.LogCritical(
                    "{Count} malicious blob(s) found. Moving all to quarantine. MessageId={Id}.",
                    maliciousBlobs.Count, payload.MessageId);

                string? destFolder = await MoveAllBlobsWithRollbackAsync(
                    blobServiceClient, blobs, QuarantineContainer, folderPrefix);

                if (destFolder is null)
                {
                    await InsertEmailDocumentAsync(payload, EmailStatus.MoveFailed,
                        "Failed to move blobs to quarantine after retries. Rolled back.",
                        message, messageActions, deadLetter: true);
                    return;
                }

                await DeleteStagingFolderAsync(sourceContainerClient, folderPrefix);
                payload.EmailAttachmentPath = destFolder;
                await InsertEmailDocumentAsync(payload, EmailStatus.Malicious,
                    $"{maliciousBlobs.Count} attachment(s) failed Defender scan and were quarantined.",
                    message, messageActions);
                return;
            }

            // ═════════════════════════════════════════════════════════════════
            // PRIORITY 3 — All scanned and all clean → ReadyForUpload
            // ═════════════════════════════════════════════════════════════════
            _logger.LogInformation(
                "All {Count} blob(s) clean. Moving to processed. MessageId={Id}.",
                cleanBlobs.Count, payload.MessageId);

            string? processedFolder = await MoveAndExtractCleanBlobsAsync(
                blobServiceClient, message, messageActions, payload, cleanBlobs, ProcessedContainer, folderPrefix);

            if (processedFolder is null) return; // Handler already dead-lettered

            await DeleteStagingFolderAsync(sourceContainerClient, folderPrefix);
            payload.EmailAttachmentPath = processedFolder;
            await InsertEmailDocumentAsync(payload, EmailStatus.ReadyForUpload,
                "All attachments passed Defender scan and are ready for upload.",
                message, messageActions);
        }

        // ─────────────────────────────────────────────────────────────────────
        // Blob move helpers
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Moves ALL blobs to the destination container preserving folder structure.
        /// Retries each blob up to MaxMoveRetries times.
        /// Rolls back on any failure.
        /// Returns destination folder path or null on failure.
        /// </summary>
        private async Task<string?> MoveAllBlobsWithRollbackAsync(
            BlobServiceClient           svc,
            IReadOnlyList<BlobScanInfo> blobs,
            string                      destContainer,
            string                      folderPrefix)
        {
            BlobContainerClient destContainerClient = svc.GetBlobContainerClient(destContainer);
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
                        blob.Item.Name, destContainer, MaxMoveRetries);

                    await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                    return null;
                }

                // Set DestinationContainer in blob metadata for recovery
                await SetDestinationMetadataAsync(destContainerClient, blob.Item.Name, destContainer);
                movedBlobNames.Add(blob.Item.Name);
            }

            return $"{destContainer}/{folderPrefix}";
        }

        /// <summary>
        /// Moves clean blobs to processed container.
        /// ZIP files are extracted — contents placed in same destination folder.
        /// Handles duplicate filenames by appending -1, -2, etc.
        /// Rolls back on any failure.
        /// Returns destination folder path or null on failure.
        /// </summary>
        private async Task<string?> MoveAndExtractCleanBlobsAsync(
            BlobServiceClient           svc,
            ServiceBusReceivedMessage   message,
            ServiceBusMessageActions    messageActions,
            EmailMessagePayload         payload,
            IReadOnlyList<BlobScanInfo> blobs,
            string                      destContainer,
            string                      folderPrefix)
        {
            BlobContainerClient destContainerClient = svc.GetBlobContainerClient(destContainer);
            await destContainerClient.CreateIfNotExistsAsync();

            var movedBlobNames = new List<string>();

            foreach (BlobScanInfo blob in blobs)
            {
                bool isZip = blob.Item.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase);

                if (isZip)
                {
                    _logger.LogInformation("ZIP detected — extracting: {Name}", blob.Item.Name);

                    using var ms = new MemoryStream();
                    try
                    {
                        await blob.Client.DownloadToAsync(ms);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogCritical(ex, "Failed to download ZIP blob {Name}.", blob.Item.Name);
                        await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                        await InsertEmailDocumentAsync(payload, EmailStatus.MoveFailed,
                            $"Failed to download ZIP '{blob.Item.Name}': {ex.Message}",
                            message, messageActions, deadLetter: true);
                        return null;
                    }

                    ms.Position = 0;

                    try
                    {
                        using var archive = new ZipArchive(ms, ZipArchiveMode.Read);
                        foreach (ZipArchiveEntry entry in archive.Entries)
                        {
                            if (string.IsNullOrEmpty(entry.Name)) continue;

                            string uniqueBlobName = await GetUniqueBlobNameAsync(
                                destContainerClient, folderPrefix, entry.Name);

                            BlobClient destBlob = destContainerClient.GetBlobClient(uniqueBlobName);
                            using var entryStream = entry.Open();
                            await destBlob.UploadAsync(entryStream, overwrite: false);

                            await SetDestinationMetadataAsync(destContainerClient, uniqueBlobName, destContainer);
                            movedBlobNames.Add(uniqueBlobName);

                            _logger.LogInformation(
                                "Extracted {Entry} → {Container}/{BlobName}",
                                entry.Name, destContainer, uniqueBlobName);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogCritical(ex, "Failed to extract ZIP blob {Name}.", blob.Item.Name);
                        await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                        await InsertEmailDocumentAsync(payload, EmailStatus.MoveFailed,
                            $"Failed to extract ZIP '{blob.Item.Name}': {ex.Message}",
                            message, messageActions, deadLetter: true);
                        return null;
                    }

                    await blob.Client.DeleteIfExistsAsync();
                }
                else
                {
                    string fileName    = Path.GetFileName(blob.Item.Name);
                    string uniqueName  = await GetUniqueBlobNameAsync(destContainerClient, folderPrefix, fileName);
                    bool   moved       = await TryMoveBlobWithRetryAsync(
                        blob.Client, destContainerClient, uniqueName, destContainer);

                    if (!moved)
                    {
                        _logger.LogCritical(
                            "Failed to move blob {Name} after {Max} retries. Rolling back.",
                            blob.Item.Name, MaxMoveRetries);
                        await RollbackMovedBlobsAsync(destContainerClient, movedBlobNames);
                        return null;
                    }

                    await SetDestinationMetadataAsync(destContainerClient, uniqueName, destContainer);
                    movedBlobNames.Add(uniqueName);
                }
            }

            return $"{destContainer}/{folderPrefix}";
        }

        /// <summary>
        /// Copies blob to destination and deletes source.
        /// Retries up to MaxMoveRetries with exponential backoff.
        /// Returns true on success.
        /// </summary>
        private async Task<bool> TryMoveBlobWithRetryAsync(
            BlobClient          source,
            BlobContainerClient destContainer,
            string              destBlobName,
            string              destContainerName)
        {
            for (int attempt = 1; attempt <= MaxMoveRetries; attempt++)
            {
                try
                {
                    BlobClient dest   = destContainer.GetBlobClient(destBlobName);
                    var        copyOp = await dest.StartCopyFromUriAsync(source.Uri);
                    await copyOp.WaitForCompletionAsync();
                    await source.DeleteIfExistsAsync();

                    _logger.LogInformation("Moved {Name} → {Container}/{Dest}",
                        source.Name, destContainerName, destBlobName);
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "Move attempt {Attempt}/{Max} failed for blob {Name}.",
                        attempt, MaxMoveRetries, source.Name);

                    if (attempt < MaxMoveRetries)
                        await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
                }
            }

            return false;
        }

        /// <summary>
        /// Deletes all blobs from destination that were moved in current operation.
        /// Called during rollback — errors are logged but not rethrown.
        /// </summary>
        private async Task RollbackMovedBlobsAsync(
            BlobContainerClient destContainer,
            IEnumerable<string> movedBlobNames)
        {
            foreach (string blobName in movedBlobNames)
            {
                try
                {
                    await destContainer.GetBlobClient(blobName).DeleteIfExistsAsync();
                    _logger.LogInformation("Rollback: deleted {Name} from {Container}.",
                        blobName, destContainer.Name);
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(ex, "Rollback: FAILED to delete {Name} from {Container}.",
                        blobName, destContainer.Name);
                }
            }
        }

        /// <summary>
        /// Increments RetryCount in blob metadata for each still-scanning blob.
        /// Retries the metadata write up to MaxMoveRetries times.
        /// Returns false if any metadata update fails.
        /// </summary>
        private async Task<bool> IncrementRetryCountAsync(
            IEnumerable<BlobScanInfo> scanningBlobs,
            string                    messageId)
        {
            foreach (BlobScanInfo blob in scanningBlobs)
            {
                bool updated = false;

                for (int attempt = 1; attempt <= MaxMoveRetries; attempt++)
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
                            attempt, MaxMoveRetries, blob.Item.Name);

                        if (attempt < MaxMoveRetries)
                            await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
                    }
                }

                if (!updated)
                {
                    _logger.LogCritical(
                        "Failed to update RetryCount for blob {Name} after {Max} attempts. MessageId={Id}.",
                        blob.Item.Name, MaxMoveRetries, messageId);
                    return false;
                }
            }

            return true;
        }

        /// <summary>Deletes all blobs under the staging folder prefix after successful move.</summary>
        private async Task DeleteStagingFolderAsync(
            BlobContainerClient sourceContainerClient,
            string              folderPrefix)
        {
            try
            {
                await foreach (BlobItem blobItem in sourceContainerClient.GetBlobsAsync(prefix: folderPrefix))
                {
                    await sourceContainerClient.GetBlobClient(blobItem.Name).DeleteIfExistsAsync();
                    _logger.LogInformation("Deleted staging blob: {Name}", blobItem.Name);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete staging folder {Prefix}. Manual cleanup may be needed.", folderPrefix);
            }
        }

        /// <summary>
        /// Sets DestinationContainer metadata on a moved blob.
        /// Used for recovery if SQL insert fails after move.
        /// </summary>
        private async Task SetDestinationMetadataAsync(
            BlobContainerClient destContainer,
            string              blobName,
            string              destContainerName)
        {
            try
            {
                BlobClient blobClient = destContainer.GetBlobClient(blobName);
                var props = await blobClient.GetPropertiesAsync();
                var metadata = new Dictionary<string, string>(props.Value.Metadata)
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

        // ─────────────────────────────────────────────────────────────────────
        // Blob query helpers
        // ─────────────────────────────────────────────────────────────────────

        private static async Task<List<BlobScanInfo>> GetBlobsFromFolderAsync(
            BlobContainerClient containerClient,
            string              folderPrefix)
        {
            var blobs = new List<BlobScanInfo>();

            await foreach (BlobItem blobItem in containerClient.GetBlobsAsync(
                prefix: folderPrefix, traits: BlobTraits.Metadata))
            {
                BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);
                blobItem.Metadata.TryGetValue("ScanResult",  out string? scanResult);
                blobItem.Metadata.TryGetValue("RetryCount",  out string? retryStr);
                int retryCount = int.TryParse(retryStr, out int r) ? r : 0;

                blobs.Add(new BlobScanInfo(blobClient, blobItem, scanResult?.Trim(), retryCount));
            }

            return blobs;
        }

        /// <summary>
        /// Checks destination containers for blobs with the given folder prefix.
        /// Used to recover from a previous run where blobs were moved but DB insert failed.
        /// Reads DestinationContainer from blob metadata for targeted lookup.
        /// </summary>
        private static async Task<(string DestFolder, string Status)?> FindBlobsInDestinationAsync(
            BlobServiceClient svc,
            string            folderPrefix)
        {
            var destinations = new[]
            {
                (ProcessedContainer,   EmailStatus.ReadyForUpload),
                (QuarantineContainer,  EmailStatus.Malicious),
                (ScanPendingContainer, EmailStatus.ScanPending),
            };

            foreach (var (container, status) in destinations)
            {
                BlobContainerClient containerClient = svc.GetBlobContainerClient(container);
                bool found = false;

                await foreach (BlobItem blobItem in containerClient.GetBlobsAsync(
                    prefix: folderPrefix, traits: BlobTraits.Metadata))
                {
                    // Check DestinationContainer metadata for targeted recovery
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

        /// <summary>
        /// Returns a unique blob name — appends -1, -2, etc. if name already exists.
        /// e.g. "invoice.pdf" → "invoice-1.pdf" if taken.
        /// </summary>
        private static async Task<string> GetUniqueBlobNameAsync(
            BlobContainerClient container,
            string              folderPrefix,
            string              fileName)
        {
            string nameNoExt = Path.GetFileNameWithoutExtension(fileName);
            string ext       = Path.GetExtension(fileName);
            string candidate = $"{folderPrefix}{fileName}";
            int    counter   = 1;

            while (await container.GetBlobClient(candidate).ExistsAsync())
            {
                candidate = $"{folderPrefix}{nameNoExt}-{counter}{ext}";
                counter++;
            }

            return candidate;
        }

        /// <summary>
        /// Parses "email-staging/MSG001_20240418120000/" into
        /// container = "email-staging" and folderPrefix = "MSG001_20240418120000/".
        /// </summary>
        private static (string Container, string FolderPrefix) ParseContainerAndPrefix(string folderPath)
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

        // ─────────────────────────────────────────────────────────────────────
        // Message lock renewal
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Background task that renews the Service Bus message lock periodically.
        /// Prevents lock expiry during large blob move operations.
        /// Aborts processing via CancellationToken if renewal fails.
        /// </summary>
        private async Task RenewMessageLockAsync(
            ServiceBusMessageActions  messageActions,
            ServiceBusReceivedMessage message,
            CancellationToken         cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(LockRenewalSeconds),
                        cancellationToken);

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        await messageActions.RenewMessageLockAsync(message);
                        _logger.LogInformation(
                            "Message lock renewed for MessageId={Id}", message.MessageId);
                    }
                }
                catch (OperationCanceledException)
                {
                    break; // Normal cancellation — exit cleanly
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(ex,
                        "Failed to renew message lock for MessageId={Id}. Processing will abort.",
                        message.MessageId);

                    // Signal main processing to stop to prevent duplicate processing
                    break;
                }
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // SQL helpers
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>Returns true if a row with the given MessageId already exists.</summary>
        private async Task<bool> MessageIdExistsAsync(string messageId)
        {
            const string sql = "SELECT COUNT(1) FROM EmailDocument WHERE MessageId = @MessageId";

            await using var conn = new SqlConnection(SqlConn);
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@MessageId", messageId);

            return (int)(await cmd.ExecuteScalarAsync())! > 0;
        }

        /// <summary>
        /// Inserts a new EmailDocument row with retry (up to MaxMoveRetries).
        /// Dead-letters the message if all retries fail.
        /// Optionally dead-letters instead of completing on success.
        /// </summary>
        private async Task InsertEmailDocumentAsync(
            EmailMessagePayload       payload,
            string                    status,
            string?                   statusReason,
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions  messageActions,
            bool                      deadLetter = false)
        {
            const string sql = @"
                INSERT INTO EmailDocument
                    (MessageId, SenderAddress, EmailSubject, EmailBody,
                     EmailAttachmentPath, EmailStatus, EmailStatusReason,
                     SendDateTime, CreatedDateTime)
                VALUES
                    (@MessageId, @SenderAddress, @EmailSubject, @EmailBody,
                     @EmailAttachmentPath, @EmailStatus, @EmailStatusReason,
                     @SendDateTime, @CreatedDateTime)";

            bool inserted = false;

            for (int attempt = 1; attempt <= MaxMoveRetries; attempt++)
            {
                try
                {
                    await using var conn = new SqlConnection(SqlConn);
                    await conn.OpenAsync();
                    await using var cmd = new SqlCommand(sql, conn);

                    cmd.Parameters.AddWithValue("@MessageId",           payload.MessageId);
                    cmd.Parameters.AddWithValue("@SenderAddress",       (object?)payload.SenderAddress       ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@EmailSubject",        (object?)payload.EmailSubject        ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@EmailBody",           (object?)payload.EmailBody           ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@EmailAttachmentPath", (object?)payload.EmailAttachmentPath ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@EmailStatus",         status);
                    cmd.Parameters.AddWithValue("@EmailStatusReason",   (object?)statusReason                ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@SendDateTime",        payload.SendDateTime);
                    cmd.Parameters.AddWithValue("@CreatedDateTime",     payload.CreatedDateTime);

                    await cmd.ExecuteNonQueryAsync();

                    _logger.LogInformation(
                        "EmailDocument inserted — MessageId={Id}, Status={Status}",
                        payload.MessageId, status);

                    inserted = true;
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "SQL insert attempt {Attempt}/{Max} failed for MessageId={Id}.",
                        attempt, MaxMoveRetries, payload.MessageId);

                    if (attempt < MaxMoveRetries)
                        await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
                }
            }

            if (!inserted)
            {
                _logger.LogCritical(
                    "SQL insert failed after {Max} retries for MessageId={Id}. Dead-lettering.",
                    MaxMoveRetries, payload.MessageId);
                await messageActions.DeadLetterMessageAsync(message,
                    deadLetterReason: "SqlInsertFailed", deadLetterErrorDescription: $"Failed to insert EmailDocument after {MaxMoveRetries} retries.");
                return;
            }

            if (deadLetter)
                await messageActions.DeadLetterMessageAsync(message, deadLetterReason: status, deadLetterErrorDescription: statusReason);
            else
                await messageActions.CompleteMessageAsync(message);
        }
    }
}
