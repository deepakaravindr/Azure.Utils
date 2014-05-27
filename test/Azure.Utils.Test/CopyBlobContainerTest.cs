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
    internal class BlobContainerPair
    {
        internal CloudStorageAccount Account { get; private set; }
        internal CloudBlobContainer SourceContainer { get; private set; }
        internal CloudBlobContainer DestinationContainer { get; private set; }
        internal BlobContainerPair(string accountName, string key, string srcContainerName, string destContainerName)
        {
            Account = new CloudStorageAccount(new StorageCredentials(accountName, key), useHttps: false);

            SourceContainer = Account.CreateCloudBlobClient().GetContainerReference(srcContainerName);
            DestinationContainer = Account.CreateCloudBlobClient().GetContainerReference(destContainerName);
        }
    }
    internal class BlobContainerFullPair
    {
        internal CloudStorageAccount SourceAccount { get; private set; }
        internal CloudStorageAccount DestinationAccount { get; private set; }
        internal CloudBlobContainer SourceContainer { get; private set; }
        internal CloudBlobContainer DestinationContainer { get; private set; }

        internal BlobContainerFullPair(string srcAccountName, string srcKey, string destAccountName, string destKey, string srcContainerName, string destContainerName)
        {
            SourceAccount = new CloudStorageAccount(new StorageCredentials(srcAccountName, srcKey), useHttps: false);
            DestinationAccount = new CloudStorageAccount(new StorageCredentials(destAccountName, destKey), useHttps: false);

            SourceContainer = SourceAccount.CreateCloudBlobClient().GetContainerReference(srcContainerName);
            DestinationContainer = DestinationAccount.CreateCloudBlobClient().GetContainerReference(destContainerName);
        }
    }
    public static class CopyBlobContainerTest
    {
        public static async Task TestIntraAccountCopy(string accountName, string key, string srcContainerName, string destContainerName)
        {
            try
            {
                var pair = new BlobContainerPair(accountName, key, srcContainerName, destContainerName);

                // INTRA ACCOUNT
                Console.WriteLine("To start copy...");
                await pair.DestinationContainer.CopyFrom(pair.Account, pair.SourceContainer);
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
                var pair = new BlobContainerFullPair(srcAccountName, srcKey, destAccountName, destKey, srcContainerName, destContainerName);

                // INTER ACCOUNT - INTER DC
                Console.WriteLine("To start copy...");
                await pair.DestinationContainer.CopyFrom(pair.DestinationAccount, pair.SourceAccount, pair.SourceContainer);
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
                var pair = new BlobContainerFullPair(srcAccountName, srcKey, destAccountName, destKey, srcContainerName, destContainerName);

                // INTER ACCOUNT - INTRA DC
                Console.WriteLine("To start copy...");
                await pair.DestinationContainer.CopyFrom(pair.DestinationAccount, pair.SourceAccount, pair.SourceContainer, null,
                    OverwriteOptions.Never, copyNotInDestination: true, deleteNotInSource: false, allowSetDestMetadata: false, allowSAS: false);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        /// <summary>
        /// Overwrites files that are already in destination
        /// </summary>
        public static async Task TestOverwriteAlways(string accountName, string key, string srcContainerName, string destContainerName)
        {
            try
            {
                var pair = new BlobContainerPair(accountName, key, srcContainerName, destContainerName);

                // INTRA ACCOUNT
                Console.WriteLine("To start copy...");
                await pair.DestinationContainer.CopyFrom(pair.Account, pair.SourceContainer, null, OverwriteOptions.Always, true, false, false);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        /// <summary>
        /// Does not copy files not in destination. Only overwrites files that are already present
        /// </summary>
        public static async Task TestCopyNotInDestination(string accountName, string key, string srcContainerName, string destContainerName)
        {
            try
            {
                var pair = new BlobContainerPair(accountName, key, srcContainerName, destContainerName);

                // INTRA ACCOUNT
                Console.WriteLine("To start copy...");
                await pair.DestinationContainer.CopyFrom(pair.Account, pair.SourceContainer, null, OverwriteOptions.Always, false, false, false);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        /// <summary>
        /// Deletes files NOT in source
        /// </summary>
        public static async Task TestDeleteNotInSource(string accountName, string key, string srcContainerName, string destContainerName)
        {
            try
            {
                var pair = new BlobContainerPair(accountName, key, srcContainerName, destContainerName);

                // INTRA ACCOUNT
                Console.WriteLine("To start copy...");
                await pair.DestinationContainer.CopyFrom(pair.Account, pair.SourceContainer, null, OverwriteOptions.Never, false, true, false);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        /// <summary>
        /// Copy and/or overwrite blobs just to set dest metadata
        /// </summary>
        public static async Task TestSetDestMetadata(string accountName, string key, string srcContainerName, string destContainerName)
        {
            try
            {
                var pair = new BlobContainerPair(accountName, key, srcContainerName, destContainerName);

                // INTRA ACCOUNT
                Console.WriteLine("To start copy...");
                await pair.DestinationContainer.CopyFrom(pair.Account, pair.SourceContainer, null, OverwriteOptions.Always, true, false, true);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static async Task TestOverwriteIfNewer(string accountName, string key, string srcContainerName, string destContainerName)
        {
            try
            {
                var pair = new BlobContainerPair(accountName, key, srcContainerName, destContainerName);

                // INTRA ACCOUNT
                Console.WriteLine("To start copy...");
                await pair.DestinationContainer.CopyFrom(pair.Account, pair.SourceContainer, null, OverwriteOptions.OnlyIfNewer, true, false, true);
                Console.WriteLine("Copy Started");
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
