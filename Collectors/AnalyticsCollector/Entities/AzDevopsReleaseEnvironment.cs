using System;
using System.Collections.Generic;
using System.IO;
using Kusto.Data.Common;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi.Contracts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AnalyticsCollector
{
    public class AzDevopsReleaseEnvironment : AzureDataExplorerService
    {
        private ReleaseRestAPIProvider _releaseRestApiProvider;
        private readonly string table = "ReleaseEnvironment";
        private readonly string mappingName = "ReleaseEnvironment_mapping_2";
        private readonly string organizationName;
        private readonly string projectId;
        private int BatchSize = 10000;

        public AzDevopsReleaseEnvironment(ReleaseRestAPIProvider releaseRestApiProvider, string kustoConnectionString, string aadTenantIdOrTenantName, string organizationName, string projectId)
            : base(kustoConnectionString, aadTenantIdOrTenantName)
        {
            this.organizationName = organizationName;
            this.projectId = projectId;
            this._releaseRestApiProvider = releaseRestApiProvider;
            this.CreateTableIfNotExists(table, mappingName);
        }

        public void IngestData(AzDevopsWaterMark azureAzDevopsWaterMark)
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

        private void WriteData(StreamWriter writer, string waterMark, out int continuationTokenOutput, out DateTime minCreatedDateTime)
        {
            TryParseWaterMark(waterMark, out int continuationToken, out minCreatedDateTime);
            continuationToken = 12999030;
            int count = 0;
            do
            {
                var releases = this._releaseRestApiProvider.GetReleases(DateTime.UtcNow.AddDays(-20), continuationToken, out continuationTokenOutput);
                Console.WriteLine($"ReleaseEnvironment: {continuationToken}");
                int currentCount = releases.Count;
                count += currentCount;

                if (currentCount > 0 && (continuationToken != 0 && continuationTokenOutput == 0))
                {
                    continuationToken = releases[currentCount - 1].Id + 1;
                    minCreatedDateTime = releases[currentCount - 1].CreatedOn;
                }
                else
                {
                    continuationToken = continuationTokenOutput;
                }

                foreach (var release in releases)
                {
                    var releaseFullObject = this._releaseRestApiProvider.GetRelease(release.Id);
                    foreach (var releaseEnvironment in releaseFullObject.Environments)
                    {
                        JObject jObject = JObject.FromObject(releaseEnvironment);
                        jObject.Add("OrganizationName", organizationName);
                        jObject.Add("ProjectId", projectId);
                        jObject.Add("Data", jObject);

                        writer.WriteLine(JsonConvert.SerializeObject(jObject));
                    }
                }

            } while (continuationToken != 0 && count <= BatchSize);
        }

        protected override List<Tuple<string, string>> GetColumns()
        {
            var columns = new List<Tuple<string, string>>()
            {
                new Tuple<string, string>("OrganizationName", "System.String"),
                new Tuple<string, string>("ProjectId", "System.String"),
                new Tuple<string, string>("ReleaseEnvironmentId", "System.Int64"),
                new Tuple<string, string>("ReleaseEnvironmentName", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionEnvironmentId", "System.Int64"),
                new Tuple<string, string>("ReleaseId", "System.Int64"),
                new Tuple<string, string>("ReleaseName", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionId", "System.Int32"),
                new Tuple<string, string>("ReleaseDefinitionName", "System.String"),
                new Tuple<string, string>("ReleaseDefinitionPath", "System.String"),
                new Tuple<string, string>("ModifiedOn", "System.DateTime"),
                new Tuple<string, string>("CreatedOn", "System.DateTime"),
                new Tuple<string, string>("EnvironmentOptionsAutoLinkWorkItems", "System.SByte"),
                new Tuple<string, string>("EnvironmentOptionsBadgeEnabled", "System.SByte"),
                new Tuple<string, string>("EnvironmentOptionsEmailNotificationType", "System.String"),
                new Tuple<string, string>("EnvironmentOptionsEmailRecipients", "System.String"),
                new Tuple<string, string>("EnvironmentOptionsEnableAccessToken", "System.SByte"),
                new Tuple<string, string>("EnvironmentOptionsPublishDeploymentStatus", "System.SByte"),
                new Tuple<string, string>("EnvironmentOptionsPullRequestDeploymentEnabled", "System.SByte"),
                new Tuple<string, string>("EnvironmentOptionsSkipArtifactsDownload", "System.SByte"),
                new Tuple<string, string>("EnvironmentOptionsTimeoutInMinutes", "System.Int64"),
                new Tuple<string, string>("NextScheduledUtcTime", "System.DateTime"),
                new Tuple<string, string>("OwnerDisplayName", "System.String"),
                new Tuple<string, string>("OwnerId", "System.String"),
                new Tuple<string, string>("OwnerInactive", "System.SByte"),
                new Tuple<string, string>("OwnerIsContainer", "System.String"),
                new Tuple<string, string>("OwnerUniqueName", "System.String"),
                new Tuple<string, string>("Rank", "System.Int32"),
                new Tuple<string, string>("ReleaseCreatedByDescriptor", "System.String"),
                new Tuple<string, string>("ReleaseCreatedByDisplayName", "System.String"),
                new Tuple<string, string>("ReleaseCreatedById", "System.String"),
                new Tuple<string, string>("ReleaseCreatedByIsContainer", "System.SByte"),
                new Tuple<string, string>("ReleaseCreatedByUniqueName", "System.String"),
                new Tuple<string, string>("Status", "System.String"),
                new Tuple<string, string>("TimeToDeploy", "System.Double"),
                new Tuple<string, string>("TriggerReason", "System.String"),
                new Tuple<string, string>("Data", "System.String")
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
            { ColumnName = "ReleaseEnvironmentId", JsonPath = "$.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseEnvironmentName", JsonPath = "$.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseDefinitionEnvironmentId", JsonPath = "$.DefinitionEnvironmentId" });
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
            { ColumnName = "ModifiedOn", JsonPath = "$.ModifiedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "CreatedOn", JsonPath = "$.CreatedOn" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsAutoLinkWorkItems", JsonPath = "$.EnvironmentOptions.AutoLinkWorkItems" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsBadgeEnabled", JsonPath = "$.EnvironmentOptions.BadgeEnabled" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsEmailNotificationType", JsonPath = "$.EnvironmentOptions.EmailNotificationType" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsEmailRecipients", JsonPath = "$.EnvironmentOptions.EmailRecipients" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsEnableAccessToken", JsonPath = "$.EnvironmentOptions.EnableAccessToken" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsPublishDeploymentStatus", JsonPath = "$.EnvironmentOptions.PublishDeploymentStatus" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsPullRequestDeploymentEnabled", JsonPath = "$.EnvironmentOptions.PullRequestDeploymentEnabled" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsSkipArtifactsDownload", JsonPath = "$.EnvironmentOptions.SkipArtifactsDownload" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EnvironmentOptionsTimeoutInMinutes", JsonPath = "$.EnvironmentOptions.TimeoutInMinutes" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "NextScheduledUtcTime", JsonPath = "$.NextScheduledUtcTime" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OwnerDisplayName", JsonPath = "$.Owner.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OwnerId", JsonPath = "$.Owner.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OwnerInactive", JsonPath = "$.Owner.inactive" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OwnerIsContainer", JsonPath = "$.Owner.isContainer" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OwnerUniqueName", JsonPath = "$.Owner.uniqueName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Rank", JsonPath = "$.Rank" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseCreatedByDescriptor", JsonPath = "$.ReleaseCreatedBy.Descriptor" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseCreatedByDisplayName", JsonPath = "$.ReleaseCreatedBy.DisplayName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseCreatedById", JsonPath = "$.ReleaseCreatedBy.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseCreatedByIsContainer", JsonPath = "$.ReleaseCreatedBy.IsContainer" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseCreatedByUniqueName", JsonPath = "$.ReleaseCreatedBy.uniqueName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Status", JsonPath = "$.Status" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "TimeToDeploy", JsonPath = "$.TimeToDeploy" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "TriggerReason", JsonPath = "$.TriggerReason" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Data", JsonPath = "$.Data" });
            return columnMappings;
        }

        private static bool TryParseWaterMark(string waterMark, out int continuationToken, out DateTime minModifiedDate)
        {
            continuationToken = 0;
            minModifiedDate = default(DateTime);

            if (!string.IsNullOrWhiteSpace(waterMark))
            {
                string[] waterMarks = waterMark.Split(',');

                if (!int.TryParse(waterMarks[0], out continuationToken))
                {
                    return false;
                }

                if (!DateTime.TryParse(waterMarks[1], out minModifiedDate))
                {
                    return false;
                }
            }
            else
            {
                return true;
            }

            return true;
        }
    }
}
