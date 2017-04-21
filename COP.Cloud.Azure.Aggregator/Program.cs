using System;
using COP.Cloud.Azure.Core.Configuration;
using Microsoft.Azure.WebJobs;

namespace COP.Cloud.Azure.Aggregator
{
    // To learn more about Microsoft Azure WebJobs SDK, please see https://go.microsoft.com/fwlink/?LinkID=320976
    class Program
    {
        // Please set the following connection strings in app.config for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage
        static void Main()
        {
            var config = new JobHostConfiguration();

            config.Queues.BatchSize = 32;
            config.Queues.MaxDequeueCount = 5;
            config.Queues.NewBatchThreshold = 16;
            config.Queues.MaxPollingInterval = TimeSpan.FromSeconds(5);

            StorageQueue.FromName("incoming-sensor-data").CreateIfNotExists();
            StorageQueue.FromName("aggregated-sensor-data").CreateIfNotExists();
            StorageTable.FromName("sensors").CreateIfNotExists();
            StorageTable.FromName("sensoralarms").CreateIfNotExists();

            var host = new JobHost(config);
            // The following code ensures that the WebJob will be running continuously
            host.RunAndBlock();
        }
    }
}
