using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace COP.Cloud.Azure.Core.Models
{
    public class Sensor : TableEntity
    {
        [IgnoreProperty]
        public string Id
        {
            get { return RowKey; }
            set { RowKey = value; }
        }

        [IgnoreProperty]
        public SensorType Type { get; set; }

        public string SensorTypeString
        {
            get { return Type.ToString(); }
            set { Type = (SensorType)Enum.Parse(typeof(SensorType), value); }
        }

        public double Min { get; set; }

        public double Max { get; set; }

        public DateTimeOffset LastSeen { get; set; } = DateTimeOffset.MinValue;
    }

    public enum SensorType
    {
        Temperature, Voltage
    }
}
