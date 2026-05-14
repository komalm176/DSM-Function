/// <summary>
/// Moves and extracts clean blobs (UPDATED: convert to PDF + return per-file paths)
/// 
/// - Scan blobs to the processed container.
/// - ZIPs are extracted into the same destination folder.
/// - All files are converted to PDF before upload.
/// - Handles duplicate filenames by appending -1, -2, etc.
/// - Rolls back on any failure.
/// - Returns list of individual processed blob paths (one per file), or null on failure.
/// </summary>
public async Task<List<string>?> MoveAndExtractCleanBlobsAsync(
    IReadOnlyList<BlobScanInfo> blobs,
    string folderPrefix)
{
    BlobContainerClient destContainerClient = _blobServiceClient.GetBlobContainerClient(_processedContainer);
    await destContainerClient.CreateIfNotExistsAsync();

    var movedBlobNames = new List<string>();         // for rollback
    var processedBlobPaths = new List<string>();     // one per file - returned to caller

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

/// <summary>
/// Processes a ZIP blob: downloads, extracts entries, converts each to PDF, uploads.
/// Returns true on success, false on any failure.
/// </summary>
private async Task<bool> TryProcessZipBlobAsync(
    BlobScanInfo blob,
    BlobContainerClient destContainerClient,
    string folderPrefix,
    List<string> movedBlobNames,
    List<string> processedBlobPaths)
{
    if (_logger.IsEnabled(LogLevel.Information))
    {
        _logger.LogInformation("ZIP detected - extracting: {Name}", blob.Item.Name);
    }

    // Download zip
    using var ms = new MemoryStream();
    try
    {
        await blob.Client.DownloadToAsync(ms);
    }
    catch (Exception ex)
    {
        if (_logger.IsEnabled(LogLevel.Critical))
        {
            _logger.LogCritical(ex, "Failed to download ZIP blob {Name}.", blob.Item.Name);
        }
        return false;
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
            await using (var entryMs = new MemoryStream())
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
                if (_logger.IsEnabled(LogLevel.Critical))
                {
                    _logger.LogCritical(ex, "Failed to convert ZIP entry {Entry} to PDF.", entry.Name);
                }
                return false;
            }

            // Unique PDF blob name
            string pdfEntryName = Path.GetFileNameWithoutExtension(entry.Name) + ".pdf";
            string uniqueBlobName = await GetUniqueBlobNameAsync(
                destContainerClient, folderPrefix, pdfEntryName);

            // Upload PDF
            BlobClient destBlob = destContainerClient.GetBlobClient(uniqueBlobName);
            using var pdfUploadStream = new MemoryStream(pdfBytes);
            await destBlob.UploadAsync(pdfUploadStream, overwrite: false);

            await SetDestinationMetadataAsync(
                destContainerClient, uniqueBlobName, _processedContainer);

            movedBlobNames.Add(uniqueBlobName);
            processedBlobPaths.Add($"{_processedContainer}/{uniqueBlobName}");

            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation(
                    "Extracted+converted {Entry} -> {Container}/{BlobName}",
                    entry.Name, _processedContainer, uniqueBlobName);
            }
        }
    }
    catch (Exception ex)
    {
        if (_logger.IsEnabled(LogLevel.Critical))
        {
            _logger.LogCritical(ex, "Failed to extract ZIP blob {Name}.", blob.Item.Name);
        }
        return false;
    }

    // Delete original zip from staging
    await blob.Client.DeleteIfExistsAsync();
    return true;
}

/// <summary>
/// Processes a non-ZIP blob: downloads, converts to PDF, uploads.
/// Returns true on success, false on any failure.
/// </summary>
private async Task<bool> TryProcessSingleBlobAsync(
    BlobScanInfo blob,
    BlobContainerClient destContainerClient,
    string folderPrefix,
    List<string> movedBlobNames,
    List<string> processedBlobPaths)
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
            if (_logger.IsEnabled(LogLevel.Critical))
            {
                _logger.LogCritical(ex, "Failed to download blob {Name}.", blob.Item.Name);
            }
            return false;
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
        if (_logger.IsEnabled(LogLevel.Critical))
        {
            _logger.LogCritical(ex, "Failed to convert blob {Name} to PDF.", blob.Item.Name);
        }
        return false;
    }

    // Unique PDF name
    string pdfFileName = Path.GetFileNameWithoutExtension(fileName) + ".pdf";
    string uniqueName = await GetUniqueBlobNameAsync(
        destContainerClient, folderPrefix, pdfFileName);

    // Upload PDF
    BlobClient destBlob = destContainerClient.GetBlobClient(uniqueName);
    using var pdfStream = new MemoryStream(pdfBytes);
    await destBlob.UploadAsync(pdfStream, overwrite: false);

    await SetDestinationMetadataAsync(
        destContainerClient, uniqueName, _processedContainer);

    movedBlobNames.Add(uniqueName);
    processedBlobPaths.Add($"{_processedContainer}/{uniqueName}");

    if (_logger.IsEnabled(LogLevel.Information))
    {
        _logger.LogInformation(
            "Converted+moved {Name} -> {Container}/{Dest}",
            blob.Item.Name, _processedContainer, uniqueName);
    }

    // Delete original from staging
    await blob.Client.DeleteIfExistsAsync();
    return true;
}
