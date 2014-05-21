using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Utils.Storage.Blob
{
    internal class CopyBlobContainerUtil
    {
        public const string SourceETagKey = "srcETag";
        public CloudStorageAccount SourceStorage
        {
            get;
            private set;
        }
        public CloudStorageAccount DestinationStorage
        {
            get;
            private set;
        }
        public CloudBlobContainer SourceContainer
        {
            get;
            private set;
        }
        public CloudBlobContainer DestinationContainer
        {
            get;
            private set;
        }

        private CopyBlobContainerEventSource Log
        {
            get;
            set;
        }
        internal CopyBlobContainerUtil(CloudStorageAccount sourceStorage, CloudBlobContainer sourceContainer,
            CloudStorageAccount destinationStorage, CloudBlobContainer destinationContainer)
        {
            Log = CopyBlobContainerEventSource.Log;
            SourceStorage = sourceStorage;
            SourceContainer = sourceContainer;
            DestinationStorage = destinationStorage;
            DestinationContainer = destinationContainer;
        }

        internal async Task CopyBlobContainer(string prefix,
            OverwriteOptions options = OverwriteOptions.Never,
            bool copyNotInDestination = true,
            bool deleteNotInSource = false,
            bool allowSetDestMetadata = false,
            bool allowSAS = true)
        {
            // Gather Source Blobs            
            var srcBlobs = await BlobContainerUtil.ListBlobs(SourceStorage, SourceContainer, prefix);            

            // Gather Destination Blobs
            var destBlobs = await BlobContainerUtil.ListBlobs(DestinationStorage, DestinationContainer, prefix);

            IList<string> blobsToBeCopied = null;
            if (copyNotInDestination)
            {
                blobsToBeCopied = srcBlobs.Keys.Where(b => !destBlobs.Keys.Contains(b)).ToList();
            }
            else
            {
                blobsToBeCopied = new List<string>();
            }

            IList<string> blobsToBeOverwritten = null;
            IList<string> commonBlobs = null;
            if (options == OverwriteOptions.Always || options == OverwriteOptions.OnlyIfNewer)
            {
                commonBlobs = blobsToBeOverwritten = srcBlobs.Keys.Where(b => !destBlobs.Keys.Contains(b)).ToList();
            }
            else if(options == OverwriteOptions.Never)
            {
                blobsToBeOverwritten = new List<string>();
            }

            if (options == OverwriteOptions.OnlyIfNewer)
            {
                blobsToBeOverwritten = new List<string>();
                foreach (var blobName in commonBlobs)
                {
                    var destBlob = destBlobs[blobName];
                    await destBlob.FetchAttributesAsync();

                    string srcETag;
                    bool shouldOverwrite = true;
                    if (destBlob.Metadata.TryGetValue(SourceETagKey, out srcETag))
                    {
                        var srcBlob = srcBlobs[blobName];
                        if (srcBlob.Properties.ETag.Equals(srcETag, StringComparison.OrdinalIgnoreCase))
                        {
                            shouldOverwrite = false;
                        }
                    }

                    if (shouldOverwrite)
                    {
                        blobsToBeOverwritten.Add(blobName);
                    }
                }
            }

            // Add the blobs to be overwritten to blobs to be copied
            blobsToBeCopied.Concat(blobsToBeOverwritten ?? Enumerable.Empty<string>());
            string sourceContainerSharedAccessUri = String.Empty;
            if (allowSAS)
            {
                var policy = new SharedAccessBlobPolicy();
                policy.SharedAccessStartTime = DateTimeOffset.Now;
                policy.SharedAccessExpiryTime = DateTimeOffset.Now + TimeSpan.FromHours(2) + TimeSpan.FromMinutes(2 * blobsToBeCopied.Count);
                policy.Permissions = SharedAccessBlobPermissions.Read;
                sourceContainerSharedAccessUri = SourceContainer.GetSharedAccessSignature(policy);
                Log.SharedAccessSignatureURI(sourceContainerSharedAccessUri);
            }

            foreach (string blobName in blobsToBeCopied)
            {
                var srcBlob = srcBlobs[blobName];
                var srcUri = allowSAS ? new Uri(srcBlob.Uri, sourceContainerSharedAccessUri) : srcBlob.Uri;
                await CopyPackage(srcUri, blobName);
            }

            IEnumerable<string> blobsToBeDeleted = null;
            if (deleteNotInSource)
            {
                blobsToBeDeleted = destBlobs.Keys.Where(b => !srcBlobs.Keys.Contains(b));
            }
        }
        private async Task CopyPackage(Uri sourceUri, string destinationBlobName)
        {
            var destinationBlob = DestinationContainer.GetBlockBlobReference(destinationBlobName);
            Log.StartingCopy(sourceUri.AbsoluteUri, destinationBlob.Uri.AbsoluteUri);
            await destinationBlob.StartCopyFromBlobAsync(sourceUri);
            Log.StartedCopy(sourceUri.AbsoluteUri, destinationBlob.Uri.AbsoluteUri);
        }
    }

    [EventSource(Name="Event Source for copy blob container tasks")]
    public class CopyBlobContainerEventSource : EventSource
    {
        public static readonly CopyBlobContainerEventSource Log = new CopyBlobContainerEventSource();
        public static readonly IList<EventSource> Logs = new List<EventSource>() { Log, BlobContainerEventSource.Log };

        private CopyBlobContainerEventSource() { }

        [Event(
            eventId: 21,
            Level = EventLevel.Informational,
            Task = CopyBlobContainerTasks.StartingBlobCopy,
            Opcode = EventOpcode.Start,
            Message = "Starting copy of {0} to {1}.")]
        public void StartingCopy(string source, string dest) { WriteEvent(21, source, dest); }

        [Event(
            eventId: 22,
            Level = EventLevel.Informational,
            Task = CopyBlobContainerTasks.StartingBlobCopy,
            Opcode = EventOpcode.Stop,
            Message = "Started copy of {0} to {1}.")]
        public void StartedCopy(string source, string dest) { WriteEvent(22, source, dest); }

        [Event(
            eventId: 23,
            Level = EventLevel.Informational,
            Task = CopyBlobContainerTasks.StartingBlobDelete,
            Opcode = EventOpcode.Start,
            Message = "Starting deletion of blob {0}")]
        public void StartingDelete(string blob) { WriteEvent(23, blob); }

        [Event(
            eventId: 24,
            Level = EventLevel.Informational,
            Task = CopyBlobContainerTasks.StartingBlobDelete,
            Opcode = EventOpcode.Stop,
            Message = "Started deletion of blob {0}")]
        public void StartedDelete(string blob) { WriteEvent(24, blob); }

        [Event(
            eventId: 30,
            Level = EventLevel.Informational,
            Opcode = EventOpcode.Info,
            Message = "Container's Shared Access signature is {0}")]
        public void SharedAccessSignatureURI(string sharedAccessSig) { WriteEvent(30, sharedAccessSig); }

        public static class CopyBlobContainerTasks
        {            
            public const EventTask StartingBlobCopy = (EventTask)(0x10);
            public const EventTask StartingBlobDelete = (EventTask)(0x11);
        }
    }
}