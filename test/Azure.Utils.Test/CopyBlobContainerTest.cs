using Azure.Utils.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Utils.Test
{
    public static class CopyBlobContainerTest
    {
        public static async Task TestIntraAccountCopy(string accountName, string key, string srcContainerName, string destContainerName)
        {
            try
            {
                var account = new CloudStorageAccount(new StorageCredentials(accountName, key), useHttps: false);
                CloudBlobContainer srcContainer = account.CreateCloudBlobClient().GetContainerReference(srcContainerName);
                CloudBlobContainer destContainer = account.CreateCloudBlobClient().GetContainerReference(destContainerName);

                // INTRA ACCOUNT
                Console.WriteLine("To start copy...");
                await destContainer.CopyFrom(account, srcContainer, null);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static async Task TestInterAccountInterDCCopy(string srcAccountName, string srcKey, string destAccountName, string destKey, string srcContainerName, string destContainerName)
        {
            try
            {
                var srcAccount = new CloudStorageAccount(new StorageCredentials(srcAccountName, srcKey), useHttps: false);
                var destAccount = new CloudStorageAccount(new StorageCredentials(destAccountName, destKey), useHttps: false);

                CloudBlobContainer srcContainer = srcAccount.CreateCloudBlobClient().GetContainerReference(srcContainerName);
                CloudBlobContainer destContainer = destAccount.CreateCloudBlobClient().GetContainerReference(destContainerName);

                // INTER ACCOUNT - INTER DC
                Console.WriteLine("To start copy...");
                await BlobContainerExtensions.CopyFrom(destContainer, destAccount, srcAccount, srcContainer, null);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static async Task TestInterAccountIntraDCCopy(string srcAccountName, string srcKey, string destAccountName, string destKey, string srcContainerName, string destContainerName)
        {
            try
            {
                var srcAccount = new CloudStorageAccount(new StorageCredentials(srcAccountName, srcKey), useHttps: false);
                var destAccount = new CloudStorageAccount(new StorageCredentials(destAccountName, destKey), useHttps: false);

                CloudBlobContainer srcContainer = srcAccount.CreateCloudBlobClient().GetContainerReference(srcContainerName);
                CloudBlobContainer destContainer = destAccount.CreateCloudBlobClient().GetContainerReference(destContainerName);

                // INTER ACCOUNT - INTRA DC
                Console.WriteLine("To start copy...");
                await BlobContainerExtensions.CopyFrom(destContainer, destAccount, srcAccount, srcContainer, null,
                    OverwriteOptions.Never, copyNotInDestination: true, deleteNotInSource: false, allowSetDestMetadata: false, allowSAS: false);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static async Task TestOverwriteAlways(string accountName, string key, string srcContainerName, string destContainerName)
        {
            try
            {
                var account = new CloudStorageAccount(new StorageCredentials(accountName, key), useHttps: false);
                CloudBlobContainer srcContainer = account.CreateCloudBlobClient().GetContainerReference(srcContainerName);
                CloudBlobContainer destContainer = account.CreateCloudBlobClient().GetContainerReference(destContainerName);

                // Overwrite Always
                Console.WriteLine("To start copy...");
                await destContainer.CopyFrom(account, srcContainer, null, OverwriteOptions.Always, true, false, false);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
