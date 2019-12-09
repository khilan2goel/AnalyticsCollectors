using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Kusto.Data.Common;

namespace AnalyticsCollector
{
    public class AzDevopsWaterMark : AzureDataExplorerService
    {
        private readonly string db = "axexperiments";
        private readonly string table = "WaterMarkTable";
        private readonly string mappingName = "WaterMark_mapping_2";
        private string organizationName;
        private string projectId;

        public AzDevopsWaterMark(string serviceNameAndRegion, string authority, string organization, string projectId)
            : base(serviceNameAndRegion, authority)
        {
            this.CreateTableIfNotExists(db, table, mappingName);
            this.organizationName = organization;
            this.projectId = projectId;
        }

        public string ReadWaterMark(string entityName)
        {
            Dictionary<string, string> parameter = new Dictionary<string, string>()
            {
                {"OrganizationName", organizationName },
                {"ProjectId", projectId },
                {"EntityName", entityName }
            };
            string query = $"WaterMarkTable | where OrganizationName == '{organizationName}' | where ProjectId == '{this.projectId}' | where EntityName == '{entityName}' | order by TimeStamp desc | project WaterMarkValue | take 1";

            var result = this.ExecuteQuery(this.db, query, parameter);
            return result.FirstOrDefault()?.Values.FirstOrDefault()?.ToString();
        }

        public void UpdateWaterMark(string entityName, string waterMarkValue)
        {
            using (var memStream = new MemoryStream())
            using (var writer = new StreamWriter(memStream))
            {
                // Write data to table
                WriteData(writer, entityName, waterMarkValue);

                writer.Flush();
                memStream.Seek(0, SeekOrigin.Begin);

                this.IngestData(db, table, mappingName, memStream);
            }
        }

        private void WriteData(StreamWriter writer, string entityName, string waterMark)
        {
            writer.WriteLine(
                "{{ \"OrganizationName\":\"{0}\", \"ProjectId\":\"{1}\", \"EntityName\":\"{2}\", \"WaterMarkValue\":\"{3}\", \"TimeStamp\":\"{4}\"}}",
                this.organizationName, this.projectId, entityName, waterMark, DateTime.UtcNow);
        }

        protected override List<Tuple<string, string>> GetColumns()
        {
            var columns = new List<Tuple<string, string>>()
            {
                new Tuple<string, string>("OrganizationName", "System.String"),
                new Tuple<string, string>("ProjectId", "System.String"),
                new Tuple<string, string>("TimeStamp", "System.DateTime"),
                new Tuple<string, string>("EntityName", "System.String"),
                new Tuple<string, string>("WaterMarkValue", "System.String")
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
            { ColumnName = "TimeStamp", JsonPath = "$.TimeStamp" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "EntityName", JsonPath = "$.EntityName" });
            columnMappings.Add(new JsonColumnMapping()
            { ColumnName = "WaterMarkValue", JsonPath = "$.WaterMarkValue" });
            return columnMappings;
        }
    }
}
