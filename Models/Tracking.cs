namespace IoTEdgeApp
{
    using System;
    using Newtonsoft.Json;

    public class Tracking
    {
        [JsonProperty("b")]
        public string Barcode { get; set; }

        [JsonProperty("st")]
        public int StatusId { get; set; }

        [JsonProperty("s")]
        public DateTime? SyncDate { get; set; }

        [JsonProperty("cd")]
        public string ConnectionDeviceId { get; set; }
    }
}