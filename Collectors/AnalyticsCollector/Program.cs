using System;
using System.Threading.Tasks;
using Microsoft.TeamFoundation.Core.WebApi;
using Microsoft.VisualStudio.Services.Common;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi.Clients;

namespace AnalyticsCollector
{
    class Program
    {
        static void Main(string[] args)
        {
            string alias = args[0]; // "user@microsoft.com";
            string token = args[1]; //"PAT_TOKEN";
            string kustoConnectionString = args[2]; //"axexperiments.southeastasia.kusto.windows.net";
            string aadTenantIdOrTenantName = args[3]; //"microsoft.com" or tenant GUID;
            string organizationName = args[4]; //"mseng";
            string projectName = args[5]; //"AzureDevops";

            ReleaseHttpClient releaseHttpClient = new ReleaseHttpClient(new Uri($"https://vsrm.dev.azure.com/{organizationName}"),
                new VssCredentials(new VssBasicCredential(alias, token)));

            ProjectHttpClient projectHttpClient = new ProjectHttpClient(new Uri($"https://dev.azure.com/{organizationName}"),
                new VssCredentials(new VssBasicCredential(alias, token)));

            var azDevopsReleaseProvider = new ReleaseRestAPIProvider(alias, token, releaseHttpClient, projectName);
            // var agentJobRequestAPIProvider = new AgentJobRequestAPIProvider(alias, token);
            var azDevopsProjectsProvider = new ProjectRestAPIProvider(projectHttpClient, projectName);

            var projectId = azDevopsProjectsProvider.GetProjectInfo(projectName).Id.ToString();

            // This is used for by each entity. It takes care of creating database as well if it doesn't exist.
            // TODO: Remove this dependency
            var axAzDevopsWaterMark = new AzDevopsWaterMark(kustoConnectionString, aadTenantIdOrTenantName, organizationName, projectId);

            var azDevopsDeploymentIngestor = new AzDevopsReleaseDeployment(azDevopsReleaseProvider, kustoConnectionString, aadTenantIdOrTenantName, organizationName, projectId);
            var azDevopsArtifactIngestor = new AzDevopsReleaseArtifact(azDevopsReleaseProvider, kustoConnectionString, aadTenantIdOrTenantName, organizationName, projectId);
            var azDevopReleaseDefinitionIngestor = new AzDevopsReleaseDefinition(azDevopsReleaseProvider, kustoConnectionString, aadTenantIdOrTenantName, organizationName, projectId);
            var azDevopsReleaseIngestor = new AzDevopsRelease(azDevopsReleaseProvider, kustoConnectionString, aadTenantIdOrTenantName, organizationName, projectId);
            var azDevopsReleaseEnvironmentIngestor = new AzDevopsReleaseEnvironment(azDevopsReleaseProvider, kustoConnectionString, aadTenantIdOrTenantName, organizationName, projectId);
            var azDevopsReleaseTimelineRecordIngestor = new AzDevopsReleaseTimelineRecord(azDevopsReleaseProvider, kustoConnectionString, aadTenantIdOrTenantName, organizationName, projectId);
            //var axAzDevopsAgentJobRequestsIngestor = new AzDevopsAgentJobRequests(agentJobRequestAPIProvider, clusterNameAndRegion, authority, organizationName, projectId);

            Console.WriteLine("Igestion started for Release Entities");

            Parallel.Invoke(
            () => azDevopReleaseDefinitionIngestor.IngestData(axAzDevopsWaterMark),
            () => azDevopsDeploymentIngestor.IngestData(axAzDevopsWaterMark),
            () => azDevopsReleaseIngestor.IngestData(axAzDevopsWaterMark),
            () => azDevopsArtifactIngestor.IngestData(axAzDevopsWaterMark),
            () => azDevopsReleaseEnvironmentIngestor.IngestData(axAzDevopsWaterMark),
            () => azDevopsReleaseTimelineRecordIngestor.IngestData(axAzDevopsWaterMark));

            Console.WriteLine("Igestion completed");
            Console.ReadKey();
        }
    }
}