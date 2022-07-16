namespace IoTEdgeApp
{
    using System.Collections.Generic;
    public class TrackingMessageBody
    {
        public string methodName { get; set; }

        public int responseTimeoutInSeconds { get; set; }

        public int connectTimeoutInSeconds { get; set; }

        public IEnumerable<Tracking> payload { get; set; }
    }
}