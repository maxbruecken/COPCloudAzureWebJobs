using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using COP.Cloud.Azure.Core.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace COP.Cloud.Azure.Validator
{
    public class Functions
    {
        public static async Task ValidateAggregatedSensorDataAsync([QueueTrigger("aggregated-sensor-data")] AggregatedSensorData aggregatedSensorData,
            [Queue("validated-sensor-data")] CloudQueue validatedSensorDataQueue,
            [Table("sensors")] CloudTable sensors,
            [Table("sensoralarms")] CloudTable sensorAlarms,
            TraceWriter log)
        {
            log.Info($"Incoming aggregated sensor data: sensor id {aggregatedSensorData.SensorId}.");

            if (aggregatedSensorData.AggregationType != AggregationType.Mean)
            {
                await ForwardAggregatedSensorData(aggregatedSensorData, validatedSensorDataQueue);
            }

            var sensor = sensors.CreateQuery<Sensor>().Where(s => s.RowKey == aggregatedSensorData.SensorId).ToList().SingleOrDefault();

            if (sensor != null)
            {
                await CheckSensorLastSeen(sensors, sensor);
                await ValidateAggregatedData(aggregatedSensorData, sensorAlarms, sensor);
                await ForwardAggregatedSensorData(aggregatedSensorData, validatedSensorDataQueue);
                return;
            }

            log.Error($"No sensor found for id {aggregatedSensorData.SensorId}");
        }

        private static async Task CheckSensorLastSeen(CloudTable sensors, Sensor sensor)
        {
            if (sensor.LastSeen < DateTimeOffset.UtcNow)
            {
                sensor.LastSeen = DateTimeOffset.UtcNow;
                var mergeOperation = TableOperation.Merge(sensor);
                await sensors.ExecuteAsync(mergeOperation);
            }
        }

        private static async Task ValidateAggregatedData(AggregatedSensorData aggregatedSensorData, CloudTable sensorAlarms, Sensor sensor)
        {
            if (aggregatedSensorData.Value < sensor.Min || aggregatedSensorData.Value > sensor.Max)
            {
                var sensorAlarm = new SensorAlarm { Status = AlarmStatus.InvalidData };
                var insertOperation = TableOperation.Insert(sensorAlarm);
                await sensorAlarms.ExecuteAsync(insertOperation);
            }
        }

        public static async Task CheckSensors([TimerTrigger("0 */1 * * * * *", RunOnStartup = true)] TimerInfo timerInfo,
            [Table("sensors")] CloudTable sensors,
            [Table("sensoralarms")] CloudTable sensorAlarms)
        {
            var deadLine = DateTimeOffset.UtcNow.AddMinutes(-5);
            await FireAlarmsForDeadSensors(sensors, sensorAlarms, deadLine);
            await RemoveObsoleteAlarms(sensors, sensorAlarms, deadLine);
        }

        private static async Task FireAlarmsForDeadSensors(CloudTable sensors, CloudTable sensorAlarms, DateTimeOffset deadLine)
        {
            var tasks = sensors
                .CreateQuery<Sensor>()
                .Where(s => s.LastSeen < deadLine)
                .ToList()
                .Select(s =>
                {
                    var sensorAlarm = new SensorAlarm {Status = AlarmStatus.Dead};
                    var insertOperation = TableOperation.Insert(sensorAlarm);
                    return sensorAlarms.ExecuteAsync(insertOperation);
                });
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private static async Task RemoveObsoleteAlarms(CloudTable sensors, CloudTable sensorAlarms, DateTimeOffset deadLine)
        {
            var tasks = sensors
                .CreateQuery<Sensor>()
                .Where(s => s.LastSeen > deadLine)
                .ToList()
                .SelectMany(s =>
                {
                    return sensorAlarms
                        .CreateQuery<SensorAlarm>()
                        .Where(a => a.StatusString == AlarmStatus.Dead.ToString())
                        .ToList();
                })
                .Select(a =>
                {
                    var deleteOperation = TableOperation.Delete(a);
                    return sensorAlarms.ExecuteAsync(deleteOperation);
                });
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private static async Task ForwardAggregatedSensorData(AggregatedSensorData aggregatedSensorData, CloudQueue validatedSensorDataQueue)
        {
            await validatedSensorDataQueue.AddMessageAsync(new CloudQueueMessage(JsonConvert.SerializeObject(aggregatedSensorData)));
        }
    }
}
