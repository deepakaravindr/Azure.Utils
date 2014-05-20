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
    internal static class BlobContainerUtil
    {
        internal static async Task<IDictionary<string, ICloudBlob>> ListBlobs(CloudStorageAccount account, CloudBlobContainer container, string prefix)
        {
            BlobContainerEventSource.Log.GatheringListOfBlobs(account.BlobEndpoint.ToString(), container.Name);
            if (await container.CreateIfNotExistsAsync())
            {
                // The container did not exist. Just created it. Returning an emtpy dictionary
                return new Dictionary<string, ICloudBlob>();
            }

            var results = new Dictionary<string, ICloudBlob>();
            BlobContinuationToken token = new BlobContinuationToken();
            BlobResultSegment segment;
            var blobRequestOptions = new BlobRequestOptions();
            var operationContext = new OperationContext();
            do
            {
                segment = await container.ListBlobsSegmentedAsync(
                    prefix: prefix,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.None,
                    maxResults: null,
                    currentToken: token,
                    options: blobRequestOptions,
                    operationContext: operationContext);

                AddToDictionary(results,
                    segment
                        .Results
                        .OfType<ICloudBlob>());

                BlobContainerEventSource.Log.GatheredBlobListSegment(account.Credentials.AccountName, container.Name, results.Count);
                token = segment.ContinuationToken;
            } while (token != null);

            BlobContainerEventSource.Log.GatheredListOfBlobs(results.Count, account.BlobEndpoint.ToString(), container.Name);
            return results;
        }

        private static void AddToDictionary(IDictionary<string, ICloudBlob> dictionary, IEnumerable<ICloudBlob> items)
        {
            foreach (var item in items)
            {
                if (dictionary.ContainsKey(item.Name))
                {
                    dictionary[item.Name] = item;
                }
                else
                {
                    dictionary.Add(item.Name, item);
                }
            }
        }
    }

    public class BlobContainerEventSource : EventSource
    {
        public static readonly BlobContainerEventSource Log = new BlobContainerEventSource();
        private BlobContainerEventSource() { }
        [Event(
            eventId: 1,
            Level = EventLevel.Informational,
            Task = BlobContainerTasks.GatheringBlobs,
            Opcode = EventOpcode.Start,
            Message = "Gathering list of blobs from account {0}/{1}")]
        public void GatheringListOfBlobs(string account, string container) { WriteEvent(1, account, container); }

        [Event(
            eventId: 2,
            Level = EventLevel.Informational,
            Task = BlobContainerTasks.GatheringBlobs,
            Opcode = EventOpcode.Stop,
            Message = "Gathered {0} blobs from account {1}/{2}")]
        public void GatheredListOfBlobs(int gathered, string account, string container) { WriteEvent(2, gathered, account, container); }

        [Event(
            eventId: 3,
            Level = EventLevel.Informational,
            Task = BlobContainerTasks.GatheringBlobsSegment,
            Opcode = EventOpcode.Receive,
            Message = "Retrieved {2} blobs in this blobs segment from {0}/{1}")]
        public void GatheredBlobListSegment(string account, string container, int totalSoFar) { WriteEvent(3, account, container, totalSoFar); }

        public static class BlobContainerTasks
        {
            public const EventTask GatheringBlobs = (EventTask)0x1;
            public const EventTask GatheringBlobsSegment = (EventTask)0x2;
            public const EventTask Max = (EventTask)0x3;
        }
    }
}
