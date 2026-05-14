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

            bool entrySuccess = await TryProcessZipEntryAsync(
                entry, destContainerClient, folderPrefix, movedBlobNames, processedBlobPaths);

            if (!entrySuccess) return false;
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
/// Processes a single entry inside a ZIP archive: reads, converts to PDF, uploads.
/// Returns true on success, false on any failure.
/// </summary>
private async Task<bool> TryProcessZipEntryAsync(
    ZipArchiveEntry entry,
    BlobContainerClient destContainerClient,
    string folderPrefix,
    List<string> movedBlobNames,
    List<string> processedBlobPaths)
{
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

    return true;
}
