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
            string clusterNameAndRegion = args[2]; //"axexperiments.southeastasia";
            string authority = args[3]; //"microsoft.com";
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

            var azDevopsDeploymentIngestor = new AzDevopsReleaseDeployment(azDevopsReleaseProvider, clusterNameAndRegion, authority, organizationName, projectId);
            var azDevopsArtifactIngestor = new AzDevopsReleaseArtifact(azDevopsReleaseProvider, clusterNameAndRegion, authority, organizationName, projectId);
            var azDevopReleaseDefinitionIngestor = new AzDevopsReleaseDefinition(azDevopsReleaseProvider, clusterNameAndRegion, authority, organizationName, projectId);
            var azDevopsReleaseIngestor = new AzDevopsRelease(azDevopsReleaseProvider, clusterNameAndRegion, authority, organizationName, projectId);
            var azDevopsReleaseEnvironmentIngestor = new AzDevopsReleaseEnvironment(azDevopsReleaseProvider, clusterNameAndRegion, authority, organizationName, projectId);
            var azDevopsReleaseTimelineRecordIngestor = new AzDevopsReleaseTimelineRecord(azDevopsReleaseProvider, clusterNameAndRegion, authority, organizationName, projectId);
            var axAzDevopsWaterMark = new AzDevopsWaterMark(clusterNameAndRegion, authority, organizationName, projectId);
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
        }
    }
}