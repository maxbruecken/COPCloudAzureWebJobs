using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using COP.Cloud.Azure.Core.Configuration;
using COP.Cloud.Azure.Core.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage.Table;

namespace COP.Cloud.Azure.FakeSensors
{
    // To learn more about Microsoft Azure WebJobs SDK, please see https://go.microsoft.com/fwlink/?LinkID=320976
    class Program
    {
        // Please set the following connection strings in app.config for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage
        static void Main()
        {
            var config = new JobHostConfiguration();

            config.UseTimers();
            StorageTable.FromName("sensors").CreateIfNotExists(CreateSensors);

            var host = new JobHost(config);
            // The following code ensures that the WebJob will be running continuously
            host.RunAndBlock();
        }

        private static void CreateSensors(CloudTable sensors)
        {
            if (sensors.CreateQuery<Sensor>().FirstOrDefault() != null) return;
            foreach (var _ in Enumerable.Range(0, 20))
            {
                var sensor = new Sensor { Id = Guid.NewGuid().ToString(), Type = SensorType.Temperature, Min = -40, Max = 100 };
                var insertOperaton = TableOperation.Insert(sensor);
                sensors.Execute(insertOperaton);
                sensor = new Sensor { Id = Guid.NewGuid().ToString(), Type = SensorType.Voltage, Min = 384, Max = 404 };
                insertOperaton = TableOperation.Insert(sensor);
                sensors.Execute(insertOperaton);
            }
        }
    }
}
