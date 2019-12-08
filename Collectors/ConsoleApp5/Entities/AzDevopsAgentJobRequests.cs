using System;
using System.Collections.Generic;
using System.IO;
using Kusto.Data.Common;
using Newtonsoft.Json;

namespace AzureDataExplorer
{
    public class AzDevopsAgentJobRequests : AzureDataExplorerService
    {
        private AgentJobRequestAPIProvider _agentsRequestRestApiProvider;
        private readonly string db = "axexperiments";
        private readonly string table = "AgentJobRequests";
        private readonly string mappingName = "AgentJobRequests_mapping_2";
        private readonly string organizationName;
        private readonly string projectId;
        private int BatchSize = 10000;

        public AzDevopsAgentJobRequests(AgentJobRequestAPIProvider agentsRequestRestApiProvider, string serviceNameAndRegion, string authority, string organizationName, string projectId)
            : base(serviceNameAndRegion, authority)
        {
            this.organizationName = organizationName;
            this.projectId = projectId;
            this._agentsRequestRestApiProvider = agentsRequestRestApiProvider;
            this.CreateTableIfNotExists(db, table, mappingName);
        }

        public void IngestData(int poolId)
        {
            using (var memStream = new MemoryStream())
            using (var writer = new StreamWriter(memStream))
            {
                // Write data to table
                WriteData(writer, poolId);

                writer.Flush();
                memStream.Seek(0, SeekOrigin.Begin);

                this.IngestData(db, table, mappingName, memStream);
            }
        }

        private void WriteData(StreamWriter writer, int poolId)
        {
            var agentJobRequests = this._agentsRequestRestApiProvider.GetAgentJobRequests(poolId);
            foreach (var agentJobRequest in agentJobRequests)
            {
                agentJobRequest.Add("OrganizationName", organizationName);
                agentJobRequest.Add("ProjectId", projectId);

                writer.WriteLine(JsonConvert.SerializeObject(agentJobRequest));
            }
        }

        protected override List<Tuple<string, string>> GetColumns()
        {
            var columns = new List<Tuple<string, string>>()
            {
                new Tuple<string, string>("OrganizationName", "System.String"),
                new Tuple<string, string>("ProjectId", "System.String"),
                new Tuple<string, string>("RequestId", "System.Int64"),
                new Tuple<string, string>("QueueTime", "System.DateTime"),
                new Tuple<string, string>("AssignTime", "System.DateTime"),
                new Tuple<string, string>("ReceiveTime", "System.DateTime"),
                new Tuple<string, string>("FinishTime", "System.DateTime"),
                new Tuple<string, string>("Result", "System.String"),
                new Tuple<string, string>("ServiceOwner", "System.String"),
                new Tuple<string, string>("PlanType", "System.String"),
                new Tuple<string, string>("PlanId", "System.String"),
                new Tuple<string, string>("JobId", "System.String"),
                new Tuple<string, string>("OwnerId", "System.Int64"),
                new Tuple<string, string>("OwnerName", "System.String"),
                new Tuple<string, string>("PoolId", "System.Int32"),
                new Tuple<string, string>("AgentDelays", "System.String"),
                new Tuple<string, string>("OrchestrationId", "System.String"),
                new Tuple<string, string>("DefinitionId", "System.Int32"),
                new Tuple<string, string>("DefinitionName", "System.String")
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
            { ColumnName = "RequestId", JsonPath = "$.requestId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "QueueTime", JsonPath = "$.queueTime" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "AssignTime", JsonPath = "$.assignTime" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ReceiveTime", JsonPath = "$.receiveTime" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "FinishTime", JsonPath = "$.finishTime" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "Result", JsonPath = "$.result" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "ServiceOwner", JsonPath = "$.serviceOwner" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "PlanType", JsonPath = "$.planType" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "PlanId", JsonPath = "$.planId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "JobId", JsonPath = "$.jobId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OwnerId", JsonPath = "$.owner.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OwnerName", JsonPath = "$.owner.name" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "PoolId", JsonPath = "$.poolId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "AgentDelays", JsonPath = "$.agentDelays" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "OrchestrationId", JsonPath = "$.orchestrationId" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionId", JsonPath = "$.definition.id" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "DefinitionName", JsonPath = "$.definition.name" });
            return columnMappings;
        }
    }
}
