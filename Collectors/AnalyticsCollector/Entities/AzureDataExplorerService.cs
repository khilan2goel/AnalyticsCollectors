using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;

namespace AnalyticsCollector
{
    public abstract class AzureDataExplorerService
    {
        private string _kustoConnectionString;
        private string _aadTenantIdOrTenantName;

        protected AzureDataExplorerService(string kustoConnectionString, string _aadTenantIdOrTenantName)
        {
            this._kustoConnectionString = kustoConnectionString; 
            this._aadTenantIdOrTenantName = _aadTenantIdOrTenantName; 
        }

        protected abstract List<Tuple<string, string>> GetColumns();

        protected abstract List<JsonColumnMapping> GetJsonColumnMappings();

        public void IngestData(string db, string table, string mappingName, Stream memStream)
        {
            // Create Ingest Client
            var kcsbDM =
                new KustoConnectionStringBuilder($"https://ingest-{_kustoConnectionString}").WithAadUserPromptAuthentication(authority: $"{_aadTenantIdOrTenantName}");

            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(kcsbDM))
            {
                var ingestProps = new KustoQueuedIngestionProperties(db, table);

                ingestProps.ReportLevel = IngestionReportLevel.FailuresAndSuccesses;
                ingestProps.ReportMethod = IngestionReportMethod.Queue;
                ingestProps.JSONMappingReference = mappingName;
                ingestProps.Format = DataSourceFormat.json;

                ingestClient.IngestFromStream(memStream, ingestProps, leaveOpen: true);

                // Wait and retrieve all notifications
                Thread.Sleep(10000);
                var errors = ingestClient.GetAndDiscardTopIngestionFailuresAsync().GetAwaiter().GetResult();
                var successes = ingestClient.GetAndDiscardTopIngestionSuccessesAsync().GetAwaiter().GetResult();

                errors.ForEach((f) => { Console.WriteLine($"Ingestion error: {f.Info.Details}"); });
                successes.ForEach((s) => { Console.WriteLine($"Ingested: {s.Info.IngestionSourcePath}"); });
            }
        }

        public void CreateTableIfNotExists(string db, string table, string mappingName)
        {
            try
            {
                // Set up table
                var kcsbEngine =
                    new KustoConnectionStringBuilder($"https://{this._kustoConnectionString}")
                        .WithAadUserPromptAuthentication(authority: $"{_aadTenantIdOrTenantName}");

                using (var kustoAdminClient = KustoClientFactory.CreateCslAdminProvider(kcsbEngine))
                {
                    // check if already exists.
                    var showTableCommands = CslCommandGenerator.GenerateTablesShowDetailsCommand();
                    var existingTables = kustoAdminClient.ExecuteControlCommand<IngestionMappingShowCommandResult>(db, showTableCommands).Select( x => x.Name).ToList();

                    if (existingTables.Contains(table))
                    {
                        Console.WriteLine($"Table {table} already exists");
                        return;
                    }

                    // Create Columns
                    var command = CslCommandGenerator.GenerateTableCreateCommand(table, GetColumns());
                    kustoAdminClient.ExecuteControlCommand(databaseName: db, command: command);

                    // Create Mapping
                    command = CslCommandGenerator.GenerateTableJsonMappingCreateCommand(
                        table, mappingName, GetJsonColumnMappings());
                    kustoAdminClient.ExecuteControlCommand(databaseName: db, command: command);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot create table due to {0}", ex);
            }
        }

        public void CreateDatabaseIfNotExists(string db)
        {
            try
            {
                // Set up Database
                var kcsbEngine =
                    new KustoConnectionStringBuilder($"https://{this._kustoConnectionString}")
                        .WithAadUserPromptAuthentication(authority: $"{_aadTenantIdOrTenantName}");

                using (var kustoAdminClient = KustoClientFactory.CreateCslAdminProvider(kcsbEngine))
                {
                    // check if database already exists.
                    var showDatabasesCommands = CslCommandGenerator.GenerateDatabasesShowCommand();
                    var existingDatabase =
                        kustoAdminClient.ExecuteControlCommand<DatabasesShowCommandResult>(showDatabasesCommands).Select(x => x.DatabaseName).ToList();

                    if (existingDatabase.Contains(db))
                    {
                        Console.WriteLine($"Database {db} already exists");
                    }
                    else
                    {
                        // Create Columns
                        var command = CslCommandGenerator.GenerateDatabaseCreateCommand(db, false, new List<string>(), false);
                        kustoAdminClient.ExecuteControlCommand(db, command);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot create database due to {0}", ex);
                throw;
            }
        }

        public IEnumerable<IDictionary<string, object>> ExecuteQuery(string db, string query, Dictionary<string, string> queryParameters)
        {
            var kcsbEngine =
                new KustoConnectionStringBuilder($"https://{this._kustoConnectionString}")
                    .WithAadUserPromptAuthentication(authority: $"{_aadTenantIdOrTenantName}");

            var clientRequestProperties = new ClientRequestProperties(
                options: null,
                parameters: queryParameters);

            using (var client = KustoClientFactory.CreateCslQueryProvider(kcsbEngine))
            {
                var reader = client.ExecuteQuery(db, query, clientRequestProperties);

                var names = Enumerable.Range(0, reader.FieldCount)
                    .Select(i => reader.GetName(i))
                    .ToArray();

                while (reader.Read())
                {
                    yield return Enumerable.Range(0, reader.FieldCount)
                        .ToDictionary(i => names[i], i => reader.GetValue(i));
                }
            }
        }
    }
}