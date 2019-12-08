using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace AzureDataExplorer
{
    public class AgentJobRequestAPIProvider : HttpRestAPIProvider
    {
        public AgentJobRequestAPIProvider(string alias, string token) : base(GetClientUsingBasicAuth(alias, token))
        {
        }

        public List<JObject> GetAgentJobRequests(int poolId)
        {
            var url = $"https://dev.azure.com/mseng/_apis/distributedtask/pools/{poolId}/jobrequests";
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