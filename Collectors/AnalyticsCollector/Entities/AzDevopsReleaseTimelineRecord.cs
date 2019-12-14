using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Kusto.Data.Common;
using Microsoft.VisualStudio.Services.ReleaseManagement.WebApi.Contracts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AnalyticsCollector
{
    public class AzDevopsReleaseTimelineRecord : AzureDataExplorerService
    {
        private ReleaseRestAPIProvider _releaseRestApiProvider;
        private readonly string table = "ReleaseTimelineRecord";
        private readonly string mappingName = "ReleaseTimelineRecord_mapping_2";
        private readonly string organizationName;
        private readonly string projectId;
        private int BatchSize = 5000;

        public AzDevopsReleaseTimelineRecord(ReleaseRestAPIProvider releaseRestApiProvider, string kustoConnectionString, string aadTenantIdOrTenantName, string organizationName, string projectId)
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
                Console.WriteLine($"Not able to ingest Releasetimelinerecord entity due to {ex}");
            }
        }

        private void WriteData(StreamWriter writer, string waterMark, out int continuationToken, out DateTime minCreatedDateTime)
        {
            ParsingHelper.TryParseWaterMark(waterMark, out continuationToken, out minCreatedDateTime);
            int count = 0;
            int currentCount;
            do
            {
                var releases = this._releaseRestApiProvider.GetReleases(minCreatedDateTime, continuationToken, out int continuationTokenOutput, ReleaseExpands.Environments);
                Console.WriteLine($"ReleaseTimelineRecord: {continuationToken}");
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

                List<string> releaseObjects = new List<string>();
                Parallel.ForEach(releases, (release) =>
                {
                    var releaseFullObject = this._releaseRestApiProvider.GetRelease(release.Id);
                    foreach (var releaseEnvironment in releaseFullObject.Environments)
                    {
                        foreach (var deployStep in releaseEnvironment.DeploySteps)
                        {
                            foreach (var phase in deployStep.ReleaseDeployPhases)
                            {
                                JObject jObject = JObject.FromObject(phase);
                                jObject.Add("OrganizationName", organizationName);
                                jObject.Add("ProjectId", projectId);
                                jObject.Add("ReleaseId", releaseEnvironment.ReleaseId);
                                jObject.Add("ReleaseEnvironmentId", releaseEnvironment.Id);
                                jObject.Add("ReleaseTimelineId", phase.RunPlanId);
                                jObject.Add("Type", "Phase");
                                releaseObjects.Add(JsonConvert.SerializeObject(jObject));

                                foreach (var job in phase.DeploymentJobs)
                                {
                                    JObject jObject2 = JObject.FromObject(job.Job);
                                    jObject2.Add("OrganizationName", organizationName);
                                    jObject2.Add("ProjectId", projectId);
                                    jObject2.Add("ReleaseId", releaseEnvironment.ReleaseId);
                                    jObject2.Add("ReleaseEnvironmentId", releaseEnvironment.Id);
                                    jObject2.Add("ParentId", phase.RunPlanId);
                                    jObject2.Add("ReleaseTimelineId", phase.RunPlanId);
                                    jObject2.Add("Type", "Job");
                                    releaseObjects.Add(JsonConvert.SerializeObject(jObject2));

                                    foreach (var task in job.Tasks)
                                    {
                                        JObject jObject3 = JObject.FromObject(task);
                                        jObject3.Add("OrganizationName", organizationName);
                                        jObject3.Add("ProjectId", projectId);
                                        jObject3.Add("ReleaseId", releaseEnvironment.ReleaseId);
                                        jObject3.Add("ReleaseEnvironmentId", releaseEnvironment.Id);
                                        jObject3.Add("ParentId", job.Job.TimelineRecordId);
                                        jObject3.Add("ReleaseTimelineId", phase.RunPlanId);
                                        jObject3.Add("Type", "Task");
                                        releaseObjects.Add(JsonConvert.SerializeObject(jObject3));
                                    }
                                }
                            }
                        }
                    }
                });

                foreach (var releaseObject in releaseObjects)
                {
                    writer.WriteLine(releaseObject);
                }

            } while (currentCount != 0 && continuationToken != 0 && count <= BatchSize);
        }

        protected override List<Tuple<string, string>> GetColumns()
        {
            var columns = new List<Tuple<string, string>>()
            {
                new Tuple<string, string>("OrganizationName", "System.String"),
                new Tuple<string, string>("ProjectId", "System.String"),
                new Tuple<string, string>("ReleaseEnvironmentId", "System.Int64"),
                new Tuple<string, string>("ReleaseId", "System.Int64"),
                new Tuple<string, string>("ReleaseTimelineId", "System.String"),
                new Tuple<string, string>("RecordId", "System.String"),
                new Tuple<string, string>("RecordName", "System.String"),
                new Tuple<string, string>("ParentId", "System.String"),
                new Tuple<string, string>("StartTime", "System.DateTime"),
                new Tuple<string, string>("FinishTime", "System.DateTime"),
                new Tuple<string, string>("LogUrl", "System.String"),
                new Tuple<string, string>("Type", "System.String"),
                new Tuple<string, string>("Status", "System.String"),
                new Tuple<string, string>("ReleaseArtifactId", "System.String"),
                new Tuple<string, string>("ArtifactType", "System.String"),
                new Tuple<string, string>("TaskId", "System.String"),
                new Tuple<string, string>("TaskName", "System.String"),
                new Tuple<string, string>("TaskVersion", "System.String"),
                new Tuple<string, string>("ErrorMessage", "System.String")
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
            { ColumnName = "ReleaseEnvironmentId", JsonPath = "$.ReleaseEnvironmentId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseId", JsonPath = "$.ReleaseId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseTimelineId", JsonPath = "$.ReleaseTimelineId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "RecordId", JsonPath = "$.TimelineRecordId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "RecordName", JsonPath = "$.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ParentId", JsonPath = "$.ParentId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Status", JsonPath = "$.Status" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "StartTime", JsonPath = "$.StartTime" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "FinishTime", JsonPath = "$.FinishTime" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "LogUrl", JsonPath = "$.LogUrl" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Type", JsonPath = "$.Type" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReleaseArtifactId", JsonPath = "$.ReleaseArtifactId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ArtifactType", JsonPath = "$.ArtifactType" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "TaskId", JsonPath = "$.Task.Id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "TaskName", JsonPath = "$.Task.Name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "TaskVersion", JsonPath = "$.Task.Version" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ErrorMessage", JsonPath = "$.Issues" });
            return columnMappings;
        }
    }
}