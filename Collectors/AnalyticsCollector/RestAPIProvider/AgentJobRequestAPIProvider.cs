using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace AnalyticsCollector
{
    public class AgentJobRequestAPIProvider : HttpRestAPIProvider
    {
        private string organizationName;

        public AgentJobRequestAPIProvider(string alias, string token, string organizationName) : base(GetClientUsingBasicAuth(alias, token))
        {
            this.organizationName = organizationName;
        }

        public List<JObject> GetAgentJobRequests(int poolId)
        {
            var url = $"https://dev.azure.com/{this.organizationName}/_apis/distributedtask/pools/{poolId}/jobrequests";
            Task<HttpResponseMessage> response = this.httpClient.GetAsync(new Uri(url));
            var statusCode = response.Result.StatusCode;

            if (statusCode == HttpStatusCode.OK)
            {
                List<JObject> jobRequests = new List<JObject>();
                HttpContent responseContent = response.Result.Content;

                string result = responseContent.ReadAsStringAsync().Result;
                var jsonResultObj = JObject.Parse(result);
                var items = jsonResultObj["value"] as JArray;

                foreach (JObject item in items)
                {
                    jobRequests.Add(item);
                }

                return jobRequests;
            }
            else
            {
                Console.WriteLine($"Got HttpStatusCode: {statusCode}");
                return null;
            }
        }

        private static HttpClient GetClientUsingBasicAuth(string alias, string token)
        {
            var client = new HttpClient();
            // Basic Auth
            var byteArray = Encoding.ASCII.GetBytes($"{alias}:{token}");
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));

            return client;
        }
    }
}