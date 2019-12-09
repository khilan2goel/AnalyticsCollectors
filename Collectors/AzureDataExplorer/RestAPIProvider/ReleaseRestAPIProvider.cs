using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi.Clients;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi.Contracts;
using Microsoft.VisualStudio.Services.WebApi;
using Newtonsoft.Json.Linq;

namespace AzureDataExplorer
{
    public class ReleaseRestAPIProvider : HttpRestAPIProvider
    {
        private ReleaseHttpClient releaseHttpClient;
        private string projectName;

        public ReleaseRestAPIProvider(string alias, string token, ReleaseHttpClient releaseHttpClient,
            string projectName) : base(GetClientUsingBasicAuth(alias, token))
        {
            this.releaseHttpClient = releaseHttpClient;
            this.projectName = projectName;
        }

        public List<Deployment> GetDeployments(DateTime minModifiedDateTime, int? continuationToken, out int nextContinuationToken)
        {
            var url = $"https://vsrm.dev.azure.com/mseng/{this.projectName}/_apis/release/deployments?queryOrder=ascending&minModifiedTime={minModifiedDateTime}&continuationToken={continuationToken}";
            Task<HttpResponseMessage> response = this.httpClient.GetAsync(new Uri(url));
            nextContinuationToken = 0;
            var statusCode = response.Result.StatusCode;

            if (statusCode == HttpStatusCode.OK)
            {
                List<Deployment> releases = new List<Deployment>();
                HttpContent responseContent = response.Result.Content;
                try
                {
                    var continuationToken2 = GetHeaderValue(response.Result, "x-ms-continuationtoken");
                    if (!string.IsNullOrEmpty(continuationToken2) && int.TryParse(continuationToken2, out int cToken))
                    {
                        nextContinuationToken = cToken;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Got Exception: {ex}");
                }

                string result = responseContent.ReadAsStringAsync().Result;
                var jsonResultObj = JObject.Parse(result);
                var items = jsonResultObj["value"] as JArray;

                foreach (JObject item in items)
                {
                    var release = item.ToObject<Deployment>();
                    releases.Add(release);
                }

                return releases;
            }
            else
            {
                Console.WriteLine($"Got HttpStatusCode: {statusCode}");
                return null;
            }
        }

        public List<Release> GetReleases(DateTime minCreatedDateTime, int continuationToken, out int nextContinuationToken, ReleaseExpands releaseexpands = ReleaseExpands.None)
        {
            var url = $"https://vsrm.dev.azure.com/mseng/{this.projectName}/_apis/release/releases?queryOrder=ascending&minCreatedTime={minCreatedDateTime}&continuationToken={continuationToken}&$expand={releaseexpands}";
            Task<HttpResponseMessage> response = this.httpClient.GetAsync(new Uri(url));
            nextContinuationToken = 0;
            var statusCode = response.Result.StatusCode;

            if (statusCode == HttpStatusCode.OK)
            {
                List<Release> releases = new List<Release>();
                HttpContent responseContent = response.Result.Content;
                try
                {
                    var continuationToken2 = GetHeaderValue(response.Result, "x-ms-continuationtoken");
                    if (!string.IsNullOrEmpty(continuationToken2) && int.TryParse(continuationToken2, out int cToken))
                    {
                        nextContinuationToken = cToken;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Got Exception: {ex}");
                }

                string result = responseContent.ReadAsStringAsync().Result;
                var jsonResultObj = JObject.Parse(result);
                var items = jsonResultObj["value"] as JArray;

                foreach (JObject item in items)
                {
                    var release = item.ToObject<Release>();
                    releases.Add(release);
                }

                return releases;
            }
            else
            {
                Console.WriteLine($"Got HttpStatusCode: {statusCode}");
                return null;
            }
        }

        public Release GetRelease(int releaseId)
        {
            return this.releaseHttpClient.GetReleaseAsync(this.projectName, releaseId: releaseId).SyncResult();
        }

        public ReleaseDefinition GetReleaseDefinition(int definitionId)
        {
            return this.releaseHttpClient.GetReleaseDefinitionAsync(this.projectName, definitionId: definitionId)
                .SyncResult();
        }

        public List<ReleaseDefinition> GetReleaseDefinitions(int? continuationToken, out int nextContinuationToken)
        {
            var url = $"https://vsrm.dev.azure.com/mseng/{this.projectName}/_apis/release/definitions?queryOrder=idAscending&continuationToken={continuationToken}";
            Task<HttpResponseMessage> response = this.httpClient.GetAsync(new Uri(url));
            nextContinuationToken = 0;
            var statusCode = response.Result.StatusCode;

            if (statusCode == HttpStatusCode.OK)
            {
                List<ReleaseDefinition> releaseDefinitions = new List<ReleaseDefinition>();
                HttpContent responseContent = response.Result.Content;
                try
                {
                    var continuationToken2 = GetHeaderValue(response.Result, "x-ms-continuationtoken");
                    if (!string.IsNullOrEmpty(continuationToken2) && int.TryParse(continuationToken2, out int cToken))
                    {
                        nextContinuationToken = cToken;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Got Exception: {ex}");
                }

                string result = responseContent.ReadAsStringAsync().Result;
                var jsonResultObj = JObject.Parse(result);
                var items = jsonResultObj["value"] as JArray;

                foreach (JObject item in items)
                {
                    var release = item.ToObject<ReleaseDefinition>();
                    releaseDefinitions.Add(release);
                }

                return releaseDefinitions;
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