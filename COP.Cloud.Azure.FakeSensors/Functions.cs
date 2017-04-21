using System;
using System.Linq;
using System.Threading.Tasks;
using COP.Cloud.Azure.Core.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace COP.Cloud.Azure.FakeSensors
{
    public class Functions
    {
        public static async Task CreateFakeSensorData([TimerTrigger("00:00:30", RunOnStartup = true)] TimerInfo timerInfo,
            [Table("sensors")] CloudTable sensors,
            [Queue("incoming-sensor-data")] CloudQueue incomingSensorDataQueue)
        {
            var random = new Random();
            var tasks = sensors
                .CreateQuery<Sensor>()
                .ToList()
                .Select(async s =>
                {
                    var sensorData = new SensorData
                    {
                        SensorId = s.Id,
                        Values = Enumerable.Range(0, 300).Select(_ => random.NextDouble() * (s.Max - s.Min) + s.Min)
                    };
                    await incomingSensorDataQueue.AddMessageAsync(new CloudQueueMessage(JsonConvert.SerializeObject(sensorData)));
                });

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }
}
