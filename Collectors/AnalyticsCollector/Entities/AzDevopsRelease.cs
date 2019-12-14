using System;
using System.Collections.Generic;
using System.IO;
using Kusto.Data.Common;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi.Contracts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AnalyticsCollector
{
    public class AzDevopsRelease : AzureDataExplorerService
    {
        private ReleaseRestAPIProvider _releaseRestApiProvider;
        private readonly string table = "Release";
        private readonly string mappingName = "Release_mapping_2";
        private readonly string organizationName;
        private readonly string projectId;
        private int BatchSize = 10000;

        public AzDevopsRelease(ReleaseRestAPIProvider releaseRestApiProvider, string kustoConnectionString, string aadTenantIdOrTenantName, string organizationName, string projectId)
            : base(kustoConnectionString, aadTenantIdOrTenantName)
        {
            this.organizationName = organizationName;
            this.projectId = projectId;
            this._releaseRestApiProvider = releaseRestApiProvider;
            this.CreateTableIfNotExists(table, mappingName);
        }

        public void IngestData(AzDevopsWaterMark azureAzDevopsWaterMark)
        {
            try
            {
                var waterMark = azureAzDevopsWaterMark.ReadWaterMark(this.table);
                int continuationToken;
                DateTime minCreatedDateTime;

                using (var memStream = new MemoryStream())
                using (var writer = new StreamWriter(memStream))
                {
                    // Write data to table
                    WriteData(writer, waterMark, out continuationToken, out minCreatedDateTime);

                    writer.Flush();
                    memStream.Seek(0, SeekOrigin.Begin);

                    this.IngestData(table, mappingName, memStream);
                }

                waterMark = string.Format("{0},{1}", continuationToken, minCreatedDateTime);
                azureAzDevopsWaterMark.UpdateWaterMark(table, waterMark);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Not able to ingest Release entity due to {ex}");
            }
        }

        private void WriteData(StreamWriter writer, string waterMark, out int continuationToken, out DateTime minCreatedDateTime)
        {
            ParsingHelper.TryParseWaterMark(waterMark, out continuationToken, out minCreatedDateTime);
            int count = 0;
            int currentCount;
            do
            {
                var releases = this._releaseRestApiProvider.GetReleases(minCreatedDateTime, continuationToken, out int continuationTokenOutput, ReleaseExpands.Variables);
                Console.WriteLine($"Release: {continuationToken}");
                currentCount = releases.Count;
                count += currentCount;
                if (currentCount > 0 && continuationTokenOutput == 0)
                {
                    continuationToken = (releases[currentCount - 1].Id) + 1;
                    minCreatedDateTime = releases[currentCount - 1].CreatedOn;
                }
                else if (continuationTokenOutput != 0)
                {
                    if (currentCount > 0 && releases[currentCount - 1].Id == continuationTokenOutput)
                    {
                        continuationToken = continuationTokenOutput + 1;
                    }
                    else
                    {
                        continuationToken = continuationTokenOutput;
                    }
                }

                foreach (var release in releases)
                {
                    JObject jObject = JObject.FromObject(release);
                    jObject.Add("OrganizationName", organizationName);
                    jObject.Add("ProjectId", projectId);

                    writer.WriteLine(JsonConvert.SerializeObject(jObject));
                }

            } while (currentCount != 0 && continuationToken != 0 && count <= BatchSize);
        }

        protected override List<Tuple<string, string>> GetColumns()
        {
            var columns = new List<Tuple<string, string>>()
            {
                new Tuple<string, string>("OrganizationName", "System.String"),
                new Tuple<string, string>("ProjectId", "System.String"),
                new Tuple<string, string>("ReleaseId", "System.Int64"),
                new Tuple<string, string>("ReleaseName", "System.String"),
                new Tuple<string, string>("CreatedByDisplayName", "System.String"),
                new Tuple<string, string>("CreatedById", "System.String"),
                new Tuple<string, string>("CreatedByIsContainer", "System.SByte"),
                new Tuple<string, string>("CreatedByUniqueName", "System.String"),
                new Tuple<string, string>("CreatedOn", "System.DateTime"),
                new Tuple<string, string>("DefinitionSnapshotRevision", "System.Int32"),
                new Tuple<string, string>("Description", "System.String"),
                new Tuple<string, string>("KeepForever", "System.SByte"),
                new Tuple<string, string>("ModifiedByDisplayName", "System.String"),
                new Tuple<string, string>("ModifiedById", "System.String"),
                new Tuple<string, string>("ModifiedByIsContainer", "System.SByte"),
                new Tuple<string, string>("ModifiedByUniqueName", "System.String"),
                new Tuple<string, string>("ModifiedOn", "System.DateTime"),
                new Tuple<string, string>("Reason", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionId", "System.Int32"),
                new Tuple<string, string>("ReleaseDefinitionName", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionPath", "System.String"),
                new Tuple<string, string>("ReleaseNameFormat", "System.String"),
                new Tuple<string, string>("Status", "System.String"),
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
            { ColumnName = "ReleaseId", JsonPath = "$.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseName", JsonPath = "$.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedByDisplayName", JsonPath = "$.CreatedBy.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedById", JsonPath = "$.CreatedBy.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedByIsContainer", JsonPath = "$.CreatedBy.isContainer" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedByUniqueName", JsonPath = "$.CreatedBy.uniqueName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedOn", JsonPath = "$.CreatedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionSnapshotRevision", JsonPath = "$.DefinitionSnapshotRevision" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Description", JsonPath = "$.Description" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "KeepForever", JsonPath = "$.KeepForever" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedByDisplayName", JsonPath = "$.ModifiedBy.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedById", JsonPath = "$.ModifiedBy.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedByIsContainer", JsonPath = "$.ModifiedBy.isContainer" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedByUniqueName", JsonPath = "$.ModifiedBy.uniqueName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ModifiedOn", JsonPath = "$.ModifiedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Reason", JsonPath = "$.Reason" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionId", JsonPath = "$.ReleaseDefinition.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionName", JsonPath = "$.ReleaseDefinition.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionPath", JsonPath = "$.ReleaseDefinition.Path" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseNameFormat", JsonPath = "$.ReleaseNameFormat" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Status", JsonPath = "$.Status" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Variables", JsonPath = "$.Variables" });
            return columnMappings;
        }
    }
}
