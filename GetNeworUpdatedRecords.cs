using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using IoTEdgeApp.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace IoTEdgeApp
{
    public static class GetNeworUpdatedRecords
    {
        private static readonly DateTime epochTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
        private static string connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
        private static string hubPolicyKey = Environment.GetEnvironmentVariable("hubPolicyKey");
        private static string hubPolicyName = Environment.GetEnvironmentVariable("hubPolicyName");
        private static string hubUrl = Environment.GetEnvironmentVariable("hubDeviceEndpoint");
        private static string hubName = Environment.GetEnvironmentVariable("hubName");
        private static bool debug = Convert.ToBoolean(Environment.GetEnvironmentVariable("debug")); 

        [FunctionName("GetNeworUpdatedRecords")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            var devices = GetDevices();

            var trackings = new List<Tracking>();
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                var getTrackings = $"SELECT Barcode, Status, SyncDate, ConnectionDeviceId FROM Tracking where CONVERT(VARCHAR, SyncDate, 101) = '01/01/1970'";
                
                if (debug)
                {
                    log.LogInformation("Get  tracking changes " + getTrackings);
                }

                conn.Open();
                using (SqlCommand cmd = new SqlCommand(getTrackings, conn))
                {
                    SqlDataReader reader = cmd.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var tracking = new Tracking
                            {
                                Barcode = CheckNull<string>(reader[0]),
                                StatusId = CheckNull<int>(reader[1]),
                                SyncDate = CheckNull<DateTime>(reader[2]),
                                ConnectionDeviceId = CheckNull<string>(reader[3])
                            };

                            trackings.Add(tracking);
                        }
                    }

                    reader.Close();
                }
            }

            if (trackings.Count > 0)
            {
                SendTrackingToIotHub(log, devices, trackings);
            }
        }

        private static void SendTrackingToIotHub(ILogger log, IEnumerable<Device> devices, List<Tracking> trackings)
        {
            log.LogInformation("Send Tracking changes to IoT Hub");

            var response = new ResponseMessage();
            
            foreach (var device in devices)
            {
                var filteredTrackings = trackings.Where(x => x.ConnectionDeviceId != device.Name);
                if (filteredTrackings.Any())
                {
                    var messageBody = new TrackingMessageBody
                    {
                        connectTimeoutInSeconds = 30,
                        responseTimeoutInSeconds = 30,
                        methodName = "InsertOrUpdateTracking",
                        payload = filteredTrackings
                    };

                    var contentMessage = JsonConvert.SerializeObject(messageBody, Formatting.None, new JsonSerializerSettings
                    {
                        NullValueHandling = NullValueHandling.Ignore,
                    });

                    var deviceUrl = new Uri(hubUrl.Replace("{deviceName}", device.Name));
                    using (var contentRequest = new StringContent(contentMessage, Encoding.UTF8, "application/json"))
                    {
                        var result = PostToIoTHub(deviceUrl, HttpMethod.Post, contentRequest);
                        log.LogInformation($"{result}");
                        response = JsonConvert.DeserializeObject<ResponseMessage>(result);
                    }
                }
            }
        }

        private static string PostToIoTHub(Uri url, HttpMethod method, HttpContent httpContent = null)
        {
            HttpResponseMessage response = null;
            using (var client = CreateHttpClient())
            {
                if (method == HttpMethod.Post)
                {
                    if (httpContent == null)
                    {
                        response = client.PostAsync(url, null).Result;
                    } else
                    {
                        response = client.PostAsync(url, httpContent).Result;
                    }
                }

                if (response.IsSuccessStatusCode)
                {
                    var adpMessageId = string.Empty;

                    var responseContent = response.Content;
                    var payload = responseContent.ReadAsStringAsync().Result;

                    return payload;
                }
            }

            return null;
        }

        private static HttpClient CreateHttpClient()
        {
            var sharedAccessSignature = SharedAccessSignature(hubName, hubPolicyName, hubPolicyKey, new TimeSpan(0, 1, 0));
            var httpClient = new HttpClient();
            
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("SharedAccessSignature", sharedAccessSignature);
            httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            httpClient.DefaultRequestHeaders.Add("ContentType", "application/json");

            return httpClient;
        }

        private static IEnumerable<Device> GetDevices()
        {
            var devices = new List<Device>();
            using (SqlConnection conn = new SqlConnection(connectionString))
            {

                conn.Open();
                var getDevices = $"SELECT Id, Name FROM Devices";
                Console.WriteLine($"{getDevices}");
                using (SqlCommand cmd = new SqlCommand(getDevices, conn))
                {
                    SqlDataReader reader = cmd.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var device = new Device
                            {
                                Id = CheckNull<Guid>(reader[0]),
                                Name = CheckNull<string>(reader[1]),
                            };

                            devices.Add(device);
                        }
                    }

                    reader.Close();
                }
            }

            return devices;
        }

        public static T CheckNull<T>(object obj)
        {
            return (obj == DBNull.Value ? default(T) : (T)obj);
        }

        static int ExecuteQuery(string sqlQuery)
        {
            int rowAffected = 0;
            Console.WriteLine(sqlQuery);

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sqlQuery, conn))
                {
                    rowAffected = cmd.ExecuteNonQuery();
                    Console.WriteLine($"{rowAffected} rows were updated");
                }
            }

            return rowAffected;
        }

        public static string SharedAccessSignature(string hostUrl, string policyName, string policyAccessKey, TimeSpan timeToLive)
        {
            if (string.IsNullOrWhiteSpace(hostUrl))
            {
                throw new ArgumentNullException(nameof(hostUrl));
            }

            var expires = Convert.ToInt64(DateTime.UtcNow.Add(timeToLive).Subtract(epochTime).TotalSeconds).ToString(CultureInfo.InvariantCulture);
            var resourceUri = WebUtility.UrlEncode(hostUrl.ToLowerInvariant());
            var toSign = string.Concat(resourceUri, "\n", expires);
            var signed = Sign(toSign, policyAccessKey);

            var sb = new StringBuilder();
            sb.Append("sr=").Append(resourceUri)
                .Append("&sig=").Append(WebUtility.UrlEncode(signed))
                .Append("&se=").Append(expires);

            if (!string.IsNullOrEmpty(policyName))
            {
                sb.Append("&skn=").Append(WebUtility.UrlEncode(policyName));
            }

            return sb.ToString();
        }

        private static string Sign(string requestString, string key)
        {
            using (var hmacshA256 = new HMACSHA256(Convert.FromBase64String(key)))
            {
                var hash = hmacshA256.ComputeHash(Encoding.UTF8.GetBytes(requestString));
                return Convert.ToBase64String(hash);
            }
        }
    }
}
