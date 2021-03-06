﻿using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace COP.Cloud.Azure.Core.Configuration
{
    public class StorageTable
    {
        private readonly string _name;

        private StorageTable(string name)
        {
            _name = name;
        }

        public static StorageTable FromName(string name)
        {
            return new StorageTable(name);
        }

        public void CreateIfNotExists(Action<CloudTable> initializer = null)
        {
            var connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Storage);
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var tableClient = storageAccount.CreateCloudTableClient();

            var cloudTable = tableClient.GetTableReference(_name);

            cloudTable.CreateIfNotExists();

            initializer?.Invoke(cloudTable);
        }
    }
}
