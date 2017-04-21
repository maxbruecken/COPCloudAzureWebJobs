using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace COP.Cloud.Azure.Core.Models
{
    public class SensorAlarm : TableEntity
    {
        [IgnoreProperty]
        public string SensorId
        {
            get { return PartitionKey; }
            set { PartitionKey = value; }
        }

        public DateTimeOffset TimeStamp { get; set; } = DateTimeOffset.UtcNow;

        [IgnoreProperty]
        public AlarmStatus Status { get; set; }

        public string StatusString
        {
            get { return Status.ToString(); }
            set { Status = (AlarmStatus) Enum.Parse(typeof(AlarmStatus), value); }
        }
    }

    public enum AlarmStatus
    {
        InvalidData, Dead, Closed
    }
}
