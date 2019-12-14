using System;
using System.Collections.Generic;
using System.IO;
using Kusto.Data.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AnalyticsCollector
{
    public class AzDevopsReleaseDeployment : AzureDataExplorerService
    {
        private ReleaseRestAPIProvider _releaseRestApiProvider;
        private string table = "ReleaseDeployment";
        private string mappingName = "ReleaseDeployment_mapping_2";
        private string organizationName;
        private string projectId;
        private int batchSize = 10000;

        public AzDevopsReleaseDeployment(ReleaseRestAPIProvider releaseRestApiProvider, string kustoConnectionString, string aadTenantIdOrTenantName, string organizationName, string projectId)
            : base(kustoConnectionString, aadTenantIdOrTenantName)
        {
            _releaseRestApiProvider = releaseRestApiProvider;
            this.organizationName = organizationName;
            this.projectId = projectId;
            CreateTableIfNotExists(table, mappingName);
        }

        public void IngestData(AzDevopsWaterMark azureAzDevopsWaterMark)
        {
            var waterMark = azureAzDevopsWaterMark.ReadWaterMark(this.table);
            int continuationToken;
            DateTime minModifiedDate;

            using (var memStream = new MemoryStream())
            using (var writer = new StreamWriter(memStream))
            {
                // Write data to table
                WriteData(writer, waterMark, out continuationToken, out minModifiedDate);

                writer.Flush();
                memStream.Seek(0, SeekOrigin.Begin);

                this.IngestData(table, mappingName, memStream);
            }

            waterMark = string.Format("{0},{1}", continuationToken, minModifiedDate);
            azureAzDevopsWaterMark.UpdateWaterMark(table, waterMark);
        }

        private void WriteData(StreamWriter writer, string waterMark, out int continuationToken, out DateTime minModifiedDate)
        {
            ParsingHelper.TryParseWaterMark(waterMark, out continuationToken, out minModifiedDate);
            int count = 0;
            int currentCount;
            do
            {
                var deployments = this._releaseRestApiProvider.GetDeployments(minModifiedDate, continuationToken, out int continuationTokenOutput);
                Console.WriteLine($"ReleaseDeployment: {continuationToken}");
                currentCount = deployments.Count;
                count += currentCount;

                if (currentCount > 0 && continuationTokenOutput == 0)
                {
                    continuationToken = deployments[currentCount - 1].Id + 1;
                    minModifiedDate = deployments[currentCount - 1].LastModifiedOn;
                }
                else if (continuationTokenOutput != 0)
                {
                    if (currentCount > 0 && deployments[currentCount - 1].Id == continuationTokenOutput)
                    {
                        continuationToken = continuationTokenOutput + 1;
                    }
                    else
                    {
                        continuationToken = continuationTokenOutput;
                    }
                }

                foreach (var deployment in deployments)
                {
                    JObject jObject = JObject.FromObject(deployment);
                    jObject.Add("OrganizationName", organizationName);
                    jObject.Add("ProjectId", projectId);

                    writer.WriteLine(JsonConvert.SerializeObject(jObject));
                }

            } while (currentCount !=0 && continuationToken != 0 && count <= batchSize);
        }

        protected override List<Tuple<string, string>> GetColumns()
        {
            var columns = new List<Tuple<string, string>>()
            {
                new Tuple<string, string>("OrganizationName", "System.String"),
                new Tuple<string, string>("ProjectId", "System.String"),
                new Tuple<string, string>("DeploymentId", "System.Int64"),
                new Tuple<string, string>("DefinitionEnvironmentId", "System.Int64"),
                new Tuple<string, string>("ReleaseId", "System.Int64"),
                new Tuple<string, string>("ReleaseName", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionId", "System.Int32"),
                new Tuple<string, string>("ReleaseDefinitionName", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionPath", "System.String"),
                new Tuple<string, string>("ReleaseEnvironmentId", "System.Int64"),
                new Tuple<string, string>("ReleaseEnvironmentName", "System.String"),
                new Tuple<string, string>("StartedOn", "System.DateTime"),
                new Tuple<string, string>("QueuedOn", "System.DateTime"),
                new Tuple<string, string>("CompletedOn", "System.DateTime"),
                new Tuple<string, string>("LastModifiedOn", "System.DateTime"),
                new Tuple<string, string>("Reason", "System.String"),
                new Tuple<string, string>("OperationStatus", "System.String"),
                new Tuple<string, string>("DeploymentStatus", "System.String"),
                new Tuple<string, string>("RequestedByDisplayName", "System.String"),
                new Tuple<string, string>("RequestedById", "System.String"),
                new Tuple<string, string>("RequestedByUniqueName", "System.String"),
                new Tuple<string, string>("RequestedForDisplayName", "System.String"),
                new Tuple<string, string>("RequestedForId", "System.String"),
                new Tuple<string, string>("RequestedForUniqueName", "System.String"),
                new Tuple<string, string>("LastModifiedByDisplayName", "System.String"),
                new Tuple<string, string>("LastModifiedById", "System.String"),
                new Tuple<string, string>("LastModifiedByUniqueName", "System.String")
            };
            return columns;
        }

        protected override List<JsonColumnMapping> GetJsonColumnMappings()
        {
            var columnMappings = new List<JsonColumnMapping>();
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OrganizationName", JsonPath = "$.OrganizationName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ProjectId", JsonPath = "$.ProjectId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DeploymentId", JsonPath = "$.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionEnvironmentId", JsonPath = "$.DefinitionEnvironmentId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseId", JsonPath = "$.Release.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseName", JsonPath = "$.Release.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionId", JsonPath = "$.ReleaseDefinition.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionName", JsonPath = "$.ReleaseDefinition.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionPath", JsonPath = "$.ReleaseDefinition.Path" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseEnvironmentId", JsonPath = "$.ReleaseEnvironment.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseEnvironmentName", JsonPath = "$.ReleaseEnvironment.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "StartedOn", JsonPath = "$.StartedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "QueuedOn", JsonPath = "$.QueuedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CompletedOn", JsonPath = "$.CompletedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "LastModifiedOn", JsonPath = "$.LastModifiedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Reason", JsonPath = "$.Reason" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OperationStatus", JsonPath = "$.OperationStatus" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DeploymentStatus", JsonPath = "$.DeploymentStatus" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "RequestedByDisplayName", JsonPath = "$.RequestedBy.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "RequestedById", JsonPath = "$.RequestedBy.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "RequestedByUniqueName", JsonPath = "$.RequestedBy.uniqueName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "RequestedForDisplayName", JsonPath = "$.RequestedFor.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "RequestedForId", JsonPath = "$.RequestedFor.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "RequestedForUniqueName", JsonPath = "$.RequestedFor.uniqueName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "LastModifiedByDisplayName", JsonPath = "$.LastModifiedBy.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "LastModifiedById", JsonPath = "$.LastModifiedBy.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "LastModifiedByUniqueName", JsonPath = "$.LastModifiedBy.uniqueName" });
            return columnMappings;
        }
    }
}