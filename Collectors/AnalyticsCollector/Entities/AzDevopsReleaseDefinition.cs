using System;
using System.Collections.Generic;
using System.IO;
using AnalyticsCollector.KustoService;
using Kusto.Data.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AnalyticsCollector
{
    public class AzDevopsReleaseDefinition : AzureDataExplorerService
    {
        private ReleaseRestAPIProvider _releaseRestApiProvider;
        private readonly string _table = "ReleaseDefinition";
        private readonly string _mappingName = "ReleaseDefinition_mapping_2";
        private readonly string _organizationName;
        private readonly string _projectId;
        private int batchSize = 10000;

        public AzDevopsReleaseDefinition(ReleaseRestAPIProvider releaseRestApiProvider, IKustoClientFactory kustoClientFactory, string organizationName, string projectId)
            : base(kustoClientFactory)
        {
            this._releaseRestApiProvider = releaseRestApiProvider;
            this._organizationName = organizationName;
            this._projectId = projectId;
            this.CreateTableIfNotExists(_table, _mappingName);
        }

        public void IngestData(AzDevopsWaterMark azureAzDevopsWaterMark)
        {
            try
            {
                var waterMark = azureAzDevopsWaterMark.ReadWaterMark(this._table);
                int continuationToken;

                using (var memStream = new MemoryStream())
                using (var writer = new StreamWriter(memStream))
                {
                    // Write data to table
                    WriteData(writer, waterMark, out continuationToken);

                    writer.Flush();
                    memStream.Seek(0, SeekOrigin.Begin);

                    this.IngestData(_table, _mappingName, memStream);
                }

                waterMark = $"{continuationToken}";
                azureAzDevopsWaterMark.UpdateWaterMark(_table, waterMark);
            }
            catch (Exception ex)
            {
                string error = $"Not able to ingest ReleaseDefinition entity due to {ex}";
                Logger.Error(error);
            }
        }

        // Release Definition doesn't change much. If already igested once, dont ingest again.
        private void WriteData(StreamWriter writer, string waterMark, out int continuationToken)
        {
            Int32.TryParse(waterMark, out continuationToken);
            Logger.Info($"Starting ingesting releasedefinition : {continuationToken}");

            int count = 0;
            int currentCount;
            do
            {
                var releaseDefinitions = this._releaseRestApiProvider.GetReleaseDefinitions(continuationToken, out int continuationTokenOutput);
                Logger.Info($"ReleaseDefinition: {continuationToken}");

                currentCount = releaseDefinitions.Count;
                count += currentCount;

                if (currentCount > 0 && continuationTokenOutput == 0)
                {
                    continuationToken = releaseDefinitions[currentCount - 1].Id + 1;
                }
                else if (continuationTokenOutput != 0)
                {
                    if (currentCount > 0 && releaseDefinitions[currentCount - 1].Id == continuationTokenOutput)
                    {
                        continuationToken = continuationTokenOutput + 1;
                    }
                    else
                    {
                        continuationToken = continuationTokenOutput;
                    }
                }

                foreach (var release in releaseDefinitions)
                {
                    JObject jObject = JObject.FromObject(release);
                    jObject.Add("OrganizationName", _organizationName);
                    jObject.Add("ProjectId", _projectId);

                    writer.WriteLine(JsonConvert.SerializeObject(jObject));
                }

            } while (currentCount != 0 && continuationToken != 0 && count <= batchSize);
        }

        protected override List<Tuple<string, string>> GetColumns()
        {
            var columns = new List<Tuple<string, string>>()
            {
                new Tuple<string, string>("OrganizationName", "System.String"),
                new Tuple<string, string>("ProjectId", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionId", "System.Int32"),
                new Tuple<string, string>("ReleaseDefinitionRevision", "System.Int64"),
                new Tuple<string, string>("ReleaseDefinitionName", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionPath", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionUrl", "System.String"),
                new Tuple<string, string>("ReleaseNameFormat", "System.String"),
                new Tuple<string, string>("IsDeleted", "System.SByte"),
                new Tuple<string, string>("Description", "System.String"),
                new Tuple<string, string>("Source", "System.String"),
                new Tuple<string, string>("CreatedByDisplayName", "System.String"),
                new Tuple<string, string>("CreatedById", "System.String"),
                new Tuple<string, string>("CreatedByUniqueName", "System.String"),
                new Tuple<string, string>("CreatedOn", "System.DateTime"),
                new Tuple<string, string>("ModifiedByDisplayName", "System.String"),
                new Tuple<string, string>("ModifiedById", "System.String"),
                new Tuple<string, string>("ModifiedByUniqueName", "System.String"),
                new Tuple<string, string>("ModifiedOn", "System.DateTime"),
                new Tuple<string, string>("PipelineProcessType", "System.String"),
                new Tuple<string, string>("Variables", "System.String")
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
            { ColumnName = "ReleaseDefinitionId", JsonPath = "$.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionRevision", JsonPath = "$.Revision" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionName", JsonPath = "$.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionPath", JsonPath = "$.Path" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionUrl", JsonPath = "$.Url" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseNameFormat", JsonPath = "$.ReleaseNameFormat" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "IsDeleted", JsonPath = "$.IsDeleted" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Description", JsonPath = "$.Description" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Source", JsonPath = "$.Source" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedByDisplayName", JsonPath = "$.CreatedBy.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedById", JsonPath = "$.CreatedBy.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedByUniqueName", JsonPath = "$.CreatedBy.uniqueName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedOn", JsonPath = "$.CreatedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedByDisplayName", JsonPath = "$.ModifiedBy.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedById", JsonPath = "$.ModifiedBy.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedByUniqueName", JsonPath = "$.ModifiedBy.uniqueName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedOn", JsonPath = "$.ModifiedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "PipelineProcessType", JsonPath = "$.PipelineProcessType" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Variables", JsonPath = "$.Variables" });
            return columnMappings;
        }
    }
}
