namespace IoTEdgeApp.Models
{
    public class ResponseMessage
    {
        public string Status { get; set; }

        public PayloadMessage Payload { get; set; }
    }
}