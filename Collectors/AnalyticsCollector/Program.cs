using System;
using System.Threading.Tasks;
using AnalyticsCollector.KustoService;
using Microsoft.TeamFoundation.Core.WebApi;
using Microsoft.VisualStudio.Services.Common;

namespace AnalyticsCollector
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string alias = args[0]; // "user@microsoft.com";
                string token = args[1]; //"PAT_TOKEN";
                string kustoConnectionString = args[2]; //"axexperiments.southeastasia.kusto.windows.net";
                string aadTenantIdOrTenantName = args[3]; //"microsoft.com" or tenant GUID;
                string organizationName = args[4]; //"mseng";
                string projectName = args[5]; //"AzureDevops";

                ProjectHttpClient projectHttpClient = new ProjectHttpClient(
                    new Uri($"https://dev.azure.com/{organizationName}"),
                    new VssCredentials(new VssBasicCredential(alias, token)));
                IKustoClientFactory kustoClientFactory =
                    new KustoClientFactory(aadTenantIdOrTenantName, kustoConnectionString);

                var azDevopsReleaseProvider = new ReleaseRestAPIProvider(alias, token, organizationName, projectName);
                var azDevopsProjectsProvider = new ProjectRestAPIProvider(projectHttpClient, projectName);

                var projectId = azDevopsProjectsProvider.GetProjectInfo(projectName).Id.ToString();

                var axAzDevopsWaterMark = new AzDevopsWaterMark(kustoClientFactory, organizationName, projectId);
                var azDevopsDeploymentIngestor = new AzDevopsReleaseDeployment(azDevopsReleaseProvider,
                    kustoClientFactory, organizationName, projectId);
                var azDevopsArtifactIngestor = new AzDevopsReleaseArtifact(azDevopsReleaseProvider, kustoClientFactory,
                    organizationName, projectId);
                var azDevopReleaseDefinitionIngestor = new AzDevopsReleaseDefinition(azDevopsReleaseProvider,
                    kustoClientFactory, organizationName, projectId);
                var azDevopsReleaseIngestor = new AzDevopsRelease(azDevopsReleaseProvider, kustoClientFactory,
                    organizationName, projectId);
                var azDevopsReleaseEnvironmentIngestor = new AzDevopsReleaseEnvironment(azDevopsReleaseProvider,
                    kustoClientFactory, organizationName, projectId);
                //var azDevopsReleaseTimelineRecordIngestor = new AzDevopsReleaseTimelineRecord(azDevopsReleaseProvider, kustoClientFactory, organizationName, projectId);

                Console.WriteLine("Ingestion started for Release Entities");
                Logger.Info("Ingestion started for Release Entities");

                Parallel.Invoke(
                    () => azDevopReleaseDefinitionIngestor.IngestData(axAzDevopsWaterMark),
                    () => azDevopsDeploymentIngestor.IngestData(axAzDevopsWaterMark),
                    () => azDevopsReleaseIngestor.IngestData(axAzDevopsWaterMark),
                    () => azDevopsArtifactIngestor.IngestData(axAzDevopsWaterMark),
                    () => azDevopsReleaseEnvironmentIngestor.IngestData(axAzDevopsWaterMark)
                    //() => azDevopsReleaseTimelineRecordIngestor.IngestData(axAzDevopsWaterMark)
                );

                Console.WriteLine(
                    "Ingestion completed. It may take around 5 minutes to reflect whole data in Azure Data explorer");
                Logger.Info("Ingestion completed. It may take around 5 minutes to reflect whole data in Azure Data explorer");

                Console.ReadKey();
            }
            catch (Exception ex)
            {
                Logger.Error($"Not able to complete ingesting due to error {ex}");
            }
        }
    }
}