using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace COP.Cloud.Azure.Core.Configuration
{
    public class StorageQueue
    {
        private readonly string _name;

        private StorageQueue(string name)
        {
            _name = name;
        }

        public static StorageQueue FromName(string name)
        {
            return new StorageQueue(name);
        }

        public void CreateIfNotExists()
        {
            var connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Storage);
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();

            var cloudQueue = queueClient.GetQueueReference(_name);

            cloudQueue.CreateIfNotExists();
        }

        public CloudQueue UnderlyingCloudQueue
        {
            get
            {
                var connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Storage);
                var storageAccount = CloudStorageAccount.Parse(connectionString);
                var queueClient = storageAccount.CreateCloudQueueClient();

                return queueClient.GetQueueReference(_name);
            }
        }
    }
}
