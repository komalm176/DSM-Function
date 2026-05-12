using System.Collections.Generic;
using System.Threading.Tasks;
using EmailUpload.Functions.Helpers;

namespace EmailUpload.Functions.Services
{
    public interface IBlobService
    {
        string ProcessedContainer    { get; }
        string QuarantineContainer   { get; }
        string ScanPendingContainer  { get; }

        Task<bool>               ContainerExistsAsync(string containerName);
        Task<List<BlobScanInfo>> GetBlobsFromFolderAsync(string containerName, string folderPrefix);

        Task<(string DestFolder, string Status)?> FindBlobsInDestinationAsync(string folderPrefix);

        Task<bool>          IncrementRetryCountAsync(IEnumerable<BlobScanInfo> scanningBlobs, string messageId);
        Task<string?>       MoveAllBlobsWithRollbackAsync(IReadOnlyList<BlobScanInfo> blobs, string destContainer, string folderPrefix);
        Task<List<string>?> MoveAndExtractCleanBlobsAsync(IReadOnlyList<BlobScanInfo> blobs, string folderPrefix); // CHANGED: string? → List<string>?
        Task                DeleteStagingFolderAsync(string containerName, string folderPrefix);
    }
}
