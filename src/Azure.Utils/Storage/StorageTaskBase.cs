using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.Utils.Storage
{
    public abstract class StorageTaskBase
    {
        public StorageTaskBase(string connectionString)
        {
            if(String.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("connectionString cannot be null or empty");
            }
            Storage = CloudStorageAccount.Parse(connectionString);
        }
        public StorageTaskBase(CloudStorageAccount storage)
        {
            if(storage == null)
            {
                throw new ArgumentNullException("storage");
            }
            Storage = storage;
        }
        public CloudStorageAccount Storage
        {
            get;
            private set;
        }
    }
}
