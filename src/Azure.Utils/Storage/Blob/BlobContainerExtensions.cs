using Azure.Utils.Storage;
using Azure.Utils.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Blob
{
    public static class BlobContainerExtensions
    {
        public static void TestLog()
        {
            CopyBlobContainerEventSource.Log.SharedAccessSignatureURI("fakeSignature");
        }

        public static void TestLogBase()
        {
            BlobContainerEventSource.Log.GatheringListOfBlobs("fakeAccountCOMPOSED", "fakeContainer");
        }

        public static async Task CopyFrom(this CloudBlobContainer container,
            CloudStorageAccount account,
            CloudBlobContainer sourceContainer,
            string prefix = null,
            OverwriteOptions options = OverwriteOptions.Never,
            bool copyNotInDestination = true,
            bool deleteNotInSource = false,
            bool allowSetDestMetadata = false)
        {
            await CopyFrom(container, account, account, sourceContainer, prefix, options, copyNotInDestination, deleteNotInSource, allowSetDestMetadata, allowSAS: false);
        }

        public static async Task CopyFrom(this CloudBlobContainer container,
            CloudStorageAccount account,
            CloudStorageAccount sourceAccount,
            CloudBlobContainer sourceContainer,
            string prefix = null,
            OverwriteOptions options = OverwriteOptions.Never,
            bool copyNotInDestination = true,
            bool deleteNotInSource = false,
            bool allowSetDestMetadata = false,
            bool allowSAS = true)
        {
            CopyBlobContainerUtil copyTask = new CopyBlobContainerUtil(sourceAccount, sourceContainer, account, container);
            await copyTask.CopyBlobContainer(prefix, options, copyNotInDestination, deleteNotInSource, allowSetDestMetadata, allowSAS);
        }
    }
}
