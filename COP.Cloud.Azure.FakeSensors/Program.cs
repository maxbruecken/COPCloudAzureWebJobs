using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using COP.Cloud.Azure.Core.Configuration;
using COP.Cloud.Azure.Core.Models;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace COP.Cloud.Azure.FakeSensors
{
    class Program
    {
        private const int SensorCount = 5;
        private static readonly TimeSpan SensorDataDelay = TimeSpan.FromSeconds(10);
        private static readonly int SensorDataCount = (int)SensorDataDelay.TotalMilliseconds / 20;

        static void Main()
        {
            var storageTable = StorageTable.FromName("sensors");
            storageTable.CreateIfNotExists(CreateSensors);

            var random = new Random();
            var cancellationTokenSource = new CancellationTokenSource();
            var incomingSensorDataQueue = StorageQueue.FromName("incoming-sensor-data").UnderlyingCloudQueue;
            var cancellationToken = cancellationTokenSource.Token;

            var sensorTasks = storageTable.UnderlyingCloudTable.CreateQuery<Sensor>()
                .ToList()
                .Select(async s =>
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var sensorData = new SensorData
                        {
                            SensorId = s.Id,
                            Values = Enumerable.Range(0, SensorDataCount).Select(_ => random.NextDouble() * (s.Max - s.Min) + s.Min)
                        };
                        try
                        {
                            await incomingSensorDataQueue.AddMessageAsync(new CloudQueueMessage(JsonConvert.SerializeObject(sensorData)), cancellationToken);
                            await Task.Delay(SensorDataDelay, cancellationToken);
                        }
                        catch (TaskCanceledException)
                        {
                        }
                        
                    }
                })
                .ToArray();
            
            Console.Out.WriteLine("Sending fake sensor data. Press any key to stop ...");
            Console.ReadKey();

            cancellationTokenSource.Cancel();

            Task.WaitAll(sensorTasks);
        }

        private static void CreateSensors(CloudTable sensors)
        {
            if (sensors.CreateQuery<Sensor>().FirstOrDefault() != null) return;
            foreach (var _ in Enumerable.Range(0, SensorCount))
            {
                var sensor = new Sensor {Id = Guid.NewGuid().ToString(), Type = SensorType.Temperature, Min = -40, Max = 100};
                var insertOperaton = TableOperation.Insert(sensor);
                sensors.Execute(insertOperaton);
                sensor = new Sensor {Id = Guid.NewGuid().ToString(), Type = SensorType.Voltage, Min = 384, Max = 404};
                insertOperaton = TableOperation.Insert(sensor);
                sensors.Execute(insertOperaton);
            }
        }
    }
}