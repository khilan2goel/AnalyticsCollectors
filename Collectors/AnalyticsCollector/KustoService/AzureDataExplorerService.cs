using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using AnalyticsCollector.KustoService;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Ingest;
using KustoClientFactory = Kusto.Data.Net.Client.KustoClientFactory;

namespace AnalyticsCollector
{
    public abstract class AzureDataExplorerService
    {
        private string _kustoConnectionString;
        private string _aadTenantIdOrTenantName;
        private string _databaseName;
        private IKustoQueuedIngestClient ingestionClient;

        protected AzureDataExplorerService(IKustoClientFactory kustoClientFactory, string kustoConnectionString, string aadTenantIdOrTenantName)
        {
            this._kustoConnectionString = kustoConnectionString;
            this._aadTenantIdOrTenantName = aadTenantIdOrTenantName;
            this.ingestionClient = kustoClientFactory.GetQueuedIngestClient();
        }

        protected abstract List<Tuple<string, string>> GetColumns();

        protected abstract List<JsonColumnMapping> GetJsonColumnMappings();

        public void IngestData(string table, string mappingName, Stream memStream)
        {
            var ingestProps =
                new KustoQueuedIngestionProperties(DatabaseName, table)
                {
                    ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                    ReportMethod = IngestionReportMethod.Queue,
                    JSONMappingReference = mappingName,
                    Format = DataSourceFormat.json
                };

            ingestionClient.IngestFromStream(memStream, ingestProps, leaveOpen: true);

            // Wait and retrieve all notifications
            Thread.Sleep(10000);
            var errors = ingestionClient.GetAndDiscardTopIngestionFailuresAsync().GetAwaiter().GetResult();
            var successes = ingestionClient.GetAndDiscardTopIngestionSuccessesAsync().GetAwaiter().GetResult();

            errors.ForEach((f) => { Console.WriteLine($"Ingestion error: {f.Info.Details}"); });
            successes.ForEach((s) => { Console.WriteLine($"Ingested: {s.Info.IngestionSourcePath}"); });
        }

        public void CreateTableIfNotExists(string table, string mappingName)
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
                    var existingTables = kustoAdminClient.ExecuteControlCommand<IngestionMappingShowCommandResult>(DatabaseName, showTableCommands).Select(x => x.Name).ToList();

                    if (existingTables.Contains(table))
                    {
                        Console.WriteLine($"Table {table} already exists");
                        return;
                    }

                    // Create Columns
                    var command = CslCommandGenerator.GenerateTableCreateCommand(table, GetColumns());
                    kustoAdminClient.ExecuteControlCommand(databaseName: DatabaseName, command: command);

                    // Create Mapping
                    command = CslCommandGenerator.GenerateTableJsonMappingCreateCommand(
                        table, mappingName, GetJsonColumnMappings());
                    kustoAdminClient.ExecuteControlCommand(databaseName: DatabaseName, command: command);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot create table due to {0}", ex);
            }
        }

        public IEnumerable<IDictionary<string, object>> ExecuteQuery(string query, Dictionary<string, string> queryParameters)
        {
            var kcsbEngine =
                new KustoConnectionStringBuilder($"https://{this._kustoConnectionString}")
                    .WithAadUserPromptAuthentication(authority: $"{_aadTenantIdOrTenantName}");

            var clientRequestProperties = new ClientRequestProperties(
                options: null,
                parameters: queryParameters);

            using (var client = KustoClientFactory.CreateCslQueryProvider(kcsbEngine))
            {
                var reader = client.ExecuteQuery(DatabaseName, query, clientRequestProperties);

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

        public string DatabaseName
        {
            get
            {
                try
                {
                    if (string.IsNullOrEmpty(this._databaseName))
                    {
                        var kcsbEngine =
                            new KustoConnectionStringBuilder($"https://{this._kustoConnectionString}")
                                .WithAadUserPromptAuthentication(authority: $"{_aadTenantIdOrTenantName}");

                        using (var kustoAdminClient = KustoClientFactory.CreateCslAdminProvider(kcsbEngine))
                        {
                            // get database name
                            var showDatabasesCommands = CslCommandGenerator.GenerateDatabasesShowCommand();
                            var existingDatabase =
                                kustoAdminClient
                                    .ExecuteControlCommand<DatabasesShowCommandResult>(showDatabasesCommands)
                                    .Select(x => x.DatabaseName).ToList();

                            this._databaseName = existingDatabase[0];
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Cannot read database due to {0}", ex);
                    throw;
                }

                return this._databaseName;
            }
        }
    }
}