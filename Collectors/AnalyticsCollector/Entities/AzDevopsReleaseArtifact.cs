using System;
using System.Collections.Generic;
using System.IO;
using Kusto.Data.Common;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi.Contracts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AnalyticsCollector
{
    public class AzDevopsReleaseArtifact : AzureDataExplorerService
    {
        private ReleaseRestAPIProvider _releaseRestApiProvider;
        private readonly string table = "ReleaseArtifact";
        private readonly string mappingName = "ReleaseArtifact_mapping_2";
        private readonly string organizationName;
        private readonly string projectId;
        private int BatchSize = 10000;

        public AzDevopsReleaseArtifact(ReleaseRestAPIProvider releaseRestApiProvider, string kustoConnectionString, string aadTenantIdOrTenantName, string organizationName, string projectId)
            : base(kustoConnectionString, aadTenantIdOrTenantName)
        {
            this._releaseRestApiProvider = releaseRestApiProvider;
            this.organizationName = organizationName;
            this.projectId = projectId;
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

        private void WriteData(StreamWriter writer, string waterMark, out int continuationToken, out DateTime minCreatedDateTime)
        {
            ParsingHelper.TryParseWaterMark(waterMark, out continuationToken, out minCreatedDateTime);
            int count = 0;
            int currentCount;
            do
            {
                var releases = this._releaseRestApiProvider.GetReleases(minCreatedDateTime, continuationToken, out int continuationTokenOutput, ReleaseExpands.Artifacts);
                Console.WriteLine($"ReleaseArtifact: {continuationToken}");
                currentCount = releases.Count;
                count += currentCount;

                if (currentCount > 0 && continuationTokenOutput == 0)
                {
                    continuationToken = releases[currentCount - 1].Id + 1;
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
                    foreach (var artifact in release.Artifacts)
                    {
                        JObject jObject = JObject.FromObject(artifact);
                        jObject.Add("OrganizationName", organizationName);
                        jObject.Add("ProjectId", projectId);
                        jObject.Add("ReleaseId", release.Id);

                        writer.WriteLine(JsonConvert.SerializeObject(jObject));
                    }
                }

            } while (currentCount !=0 && continuationToken != 0 && count <= BatchSize);
        }

        protected override List<Tuple<string, string>> GetColumns()
        {
            var columns = new List<Tuple<string, string>>()
            {
                new Tuple<string, string>("OrganizationName", "System.String"),
                new Tuple<string, string>("ProjectId", "System.String"),
                new Tuple<string, string>("ReleaseId", "System.Int64"),
                new Tuple<string, string>("SourceId", "System.String"),
                new Tuple<string, string>("Type", "System.String"),
                new Tuple<string, string>("Alias", "System.String"),
                new Tuple<string, string>("DefinitionReferenceIsTriggeringArtifactId", "System.SByte"),
                new Tuple<string, string>("DefinitionReferenceIsTriggeringArtifactName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceArtifactSourceDefinitionUrlId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceArtifactSourceVersionUrlId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceArtifactsName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceBranchId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceBranchName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceBranchesId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceBranchesName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceConnectionId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceConnectionName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceDefaultVersionTypeId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceDefaultVersionTypeName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceDefinitionId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceDefinitionName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceFeedId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceFeedName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceProjectId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceProjectName", "System.String"),
                new Tuple<string, string>("DefinitionReferencePullRequestId", "System.String"),
                new Tuple<string, string>("DefinitionReferencePullRequestIdName", "System.String"),
                new Tuple<string, string>("DefinitionReferencePullRequestMergeCommitId", "System.String"),
                new Tuple<string, string>("DefinitionReferencePullRequestMergeCommitIdName", "System.String"),
                new Tuple<string, string>("DefinitionReferencePullRequestSourceBranchCommitId", "System.String"),
                new Tuple<string, string>("DefinitionReferencePullRequestSourceBranchCommitIdName", "System.String"),
                new Tuple<string, string>("DefinitionReferenceVersionId", "System.String"),
                new Tuple<string, string>("DefinitionReferenceVersionName", "System.String"),
                new Tuple<string, string>("IsPrimary", "System.SByte")
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
            { ColumnName = "ReleaseId", JsonPath = "$.ReleaseId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "SourceId", JsonPath = "$.SourceId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Type", JsonPath = "$.Type" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Alias", JsonPath = "$.Alias" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceIsTriggeringArtifactId", JsonPath = "$.DefinitionReference.isTriggeringArtifact.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceIsTriggeringArtifactName", JsonPath = "$.DefinitionReference.isTriggeringArtifact.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceArtifactSourceDefinitionUrlId", JsonPath = "$.DefinitionReference.artifactSourceDefinitionUrl.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceArtifactSourceVersionUrlId", JsonPath = "$.DefinitionReference.artifactSourceVersionUrl.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceArtifactsName", JsonPath = "$.DefinitionReference.artifacts.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceBranchId", JsonPath = "$.DefinitionReference.branch.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceBranchName", JsonPath = "$.DefinitionReference.branch.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceBranchesId", JsonPath = "$.DefinitionReference.branches.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceBranchesName", JsonPath = "$.DefinitionReference.branches.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceConnectionId", JsonPath = "$.DefinitionReference.connection.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceConnectionName", JsonPath = "$.DefinitionReference.connection.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceDefaultVersionTypeId", JsonPath = "$.DefinitionReference.defaultVersionType.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceDefaultVersionTypeName", JsonPath = "$.DefinitionReference.defaultVersionType.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceDefinitionId", JsonPath = "$.DefinitionReference.definition.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceDefinitionName", JsonPath = "$.DefinitionReference.definition.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceFeedId", JsonPath = "$.DefinitionReference.feed.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceFeedName", JsonPath = "$.DefinitionReference.feed.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceProjectId", JsonPath = "$.DefinitionReference.project.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceProjectName", JsonPath = "$.DefinitionReference.project.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferencePullRequestId", JsonPath = "$.DefinitionReference.pullRequest.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferencePullRequestIdName", JsonPath = "$.DefinitionReference.pullRequest.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferencePullRequestMergeCommitId", JsonPath = "$.DefinitionReference.pullRequest.MergeCommit.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferencePullRequestMergeCommitIdName", JsonPath = "$.DefinitionReference.pullRequest.MergeCommit.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferencePullRequestSourceBranchCommitId", JsonPath = "$.DefinitionReference.pullRequest.SourceBranchCommit.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferencePullRequestSourceBranchCommitIdName", JsonPath = "$.DefinitionReference.pullRequest.SourceBranchCommit.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceVersionId", JsonPath = "$.DefinitionReference.version.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionReferenceVersionName", JsonPath = "$.DefinitionReference.version.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "IsPrimary", JsonPath = "$.IsPrimary" });
            return columnMappings;
        }
    }
}