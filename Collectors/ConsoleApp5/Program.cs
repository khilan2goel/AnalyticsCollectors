using System;
using Microsoft.TeamFoundation.Core.WebApi;
using Microsoft.VisualStudio.Services.Common;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi.Clients;

namespace AzureDataExplorer
{
    class Program
    {
        static void Main(string[] args)
        {
            //string username = args[0];
            //string password = args[1];
            //string clientId = args[2];
            //string clientSecret = args[3];
            //string projectName = args[4];
            //string organizationName = args[5];

            string alias = "user@microsoft.com";
            string token = "PAT_TOKEN";
            string serviceNameAndRegion = "axexperiments.southeastasia";
            string authority = "microsoft.com";

            ReleaseHttpClient releaseHttpClient = new ReleaseHttpClient(new Uri("https://vsrm.dev.azure.com/mseng"),
                new VssCredentials(new VssBasicCredential(alias,token)));

            ProjectHttpClient projectHttpClient = new ProjectHttpClient(new Uri("https://dev.azure.com/mseng"),
                new VssCredentials(new VssBasicCredential(alias, token)));

            var azDevopsReleaseProvider = new ReleaseRestAPIProvider(alias, token, releaseHttpClient, "AzureDevops");
            var agentJobRequestAPIProvider = new AgentJobRequestAPIProvider(alias, token);
            var azDevopsProjectsProvider = new ProjectRestAPIProvider(projectHttpClient, "AzureDevops");

            var projectId = azDevopsProjectsProvider.GetProjectInfo("AzureDevops").Id.ToString();

            //var azDevopsDeploymentIngestor = new AzDevopsReleaseDeployment(azDevopsReleaseProvider, serviceNameAndRegion, authority, "mseng", projectId);
            //var azDevopsArtifactIngestor = new AzDevopsReleaseArtifact(azDevopsReleaseProvider, serviceNameAndRegion, authority, "mseng", projectId);
            //var azDevopReleaseDefinitionIngestor = new AzDevopsReleaseDefinition(azDevopsReleaseProvider, serviceNameAndRegion, authority, "mseng", projectId);
            //var azDevopsReleaseIngestor = new AzDevopsRelease(azDevopsReleaseProvider, serviceNameAndRegion, authority, "mseng", projectId);
            //var azDevopsReleaseEnvironmentIngestor = new AzDevopsReleaseEnvironment(azDevopsReleaseProvider, serviceNameAndRegion, authority, "mseng", projectId);
            //var azDevopsReleaseTimelineRecordIngestor = new AzDevopsReleaseTimelineRecord(azDevopsReleaseProvider, serviceNameAndRegion, authority, "mseng", projectId);
            //var axAzDevopsWaterMark = new AzDevopsWaterMark(serviceNameAndRegion, authority, "mseng", projectId);
            var azDevopsReleaseTimelineRecordIngestor2 = new AzDevopsAgentJobRequests(agentJobRequestAPIProvider, serviceNameAndRegion, authority, "mseng", projectId);


            //azDevopReleaseDefinitionIngestor.IngestData(axAzDevopsWaterMark);
            //azDevopsDeploymentIngestor.IngestData(axAzDevopsWaterMark);
            azDevopsReleaseTimelineRecordIngestor2.IngestData(676);
            //azDevopsReleaseIngestor.IngestData(axAzDevopsWaterMark);
            //azDevopsArtifactIngestor.IngestData(axAzDevopsWaterMark);
            //azDevopsReleaseTimelineRecordIngestor.IngestData(axAzDevopsWaterMark);
        }
    }
}