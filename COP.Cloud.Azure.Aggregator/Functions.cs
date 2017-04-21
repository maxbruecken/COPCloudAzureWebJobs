﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using COP.Cloud.Azure.Core.Models;
using MathNet.Numerics.Statistics;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace COP.Cloud.Azure.Aggregator
{
    public class Functions
    {
        public static async Task ProcessSensorDataAsync([QueueTrigger("incoming-sensor-data")] SensorData sensorData,
            [Queue("aggregated-sensor-data")] CloudQueue aggregatedSensorDataQueue,
            TraceWriter log)
        {
            log.Info($"Incoming raw sensor data: sensor id {sensorData.SensorId}.");

            await AggregateDataAndSendMessage(sensorData, aggregatedSensorDataQueue, AggregationType.Mean, x => sensorData.Values.Mean());
            await AggregateDataAndSendMessage(sensorData, aggregatedSensorDataQueue, AggregationType.Min, x => sensorData.Values.Minimum());
            await AggregateDataAndSendMessage(sensorData, aggregatedSensorDataQueue, AggregationType.Max, x => sensorData.Values.Maximum());
            await AggregateDataAndSendMessage(sensorData, aggregatedSensorDataQueue, AggregationType.StandardDeviation, x => sensorData.Values.StandardDeviation());
        }

        private static async Task AggregateDataAndSendMessage(SensorData sensorData, CloudQueue aggregatedSensorDataQueue, AggregationType aggregationType, Func<IEnumerable<double>, double> aggregator)
        {
            var aggregatedSensorData = new AggregatedSensorData
            {
                SensorId = sensorData.SensorId,
                AggregationType = aggregationType,
                TimeStamp = DateTimeOffset.UtcNow,
                Value = aggregator(sensorData.Values)
            };
            await aggregatedSensorDataQueue.AddMessageAsync(new CloudQueueMessage(JsonConvert.SerializeObject(aggregatedSensorData))).ConfigureAwait(false);
        }

        public static async Task ProcessFailedSensorDataAsync([QueueTrigger("incoming-sensor-data-poison")] SensorData sensorData,
            TraceWriter log)
        {
            log.Error($"Sensor data for sensor id {sensorData.SensorId} couldn't be aggregated.");

            // ToDo: add handling of failed sensor data
        }
    }
}
