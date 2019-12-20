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
        private readonly IKustoQueuedIngestClient _ingestionClient;
        private readonly KustoConnectionStringBuilder _kustoConnectionStringBuilder;
        private string _databaseName;

        protected AzureDataExplorerService(IKustoClientFactory kustoClientFactory)
        {
            this._ingestionClient = kustoClientFactory.GetQueuedIngestClient();
            this._kustoConnectionStringBuilder = kustoClientFactory.KustoConnectionStringBuilder;
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

            _ingestionClient.IngestFromStream(memStream, ingestProps, leaveOpen: true);

            // Wait and retrieve all notifications
            Thread.Sleep(10000);
            var errors = _ingestionClient.GetAndDiscardTopIngestionFailuresAsync().GetAwaiter().GetResult();
            var successes = _ingestionClient.GetAndDiscardTopIngestionSuccessesAsync().GetAwaiter().GetResult();

            errors.ForEach((f) => { Logger.Error($"Ingestion error: {f.Info.Details}."); });
            successes.ForEach((s) => { Logger.Info($"Ingested : {s.Info.IngestionSourcePath}"); });
        }

        public void CreateTableIfNotExists(string table, string mappingName)
        {
            try
            {
                using (var kustoAdminClient = KustoClientFactory.CreateCslAdminProvider(_kustoConnectionStringBuilder))
                {
                    // check if already exists.
                    var showTableCommands = CslCommandGenerator.GenerateTablesShowDetailsCommand();
                    var existingTables = kustoAdminClient.ExecuteControlCommand<IngestionMappingShowCommandResult>(DatabaseName, showTableCommands).Select(x => x.Name).ToList();

                    if (existingTables.Contains(table))
                    {
                        Logger.Info($"Table {table} already exists");
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
                Logger.Error($"Cannot create table due to {ex}");
            }
        }

        public IEnumerable<IDictionary<string, object>> ExecuteQuery(string query, Dictionary<string, string> queryParameters)
        {
            var clientRequestProperties = new ClientRequestProperties(
                options: null,
                parameters: queryParameters);

            using (var client = KustoClientFactory.CreateCslQueryProvider(_kustoConnectionStringBuilder))
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
                        using (var kustoAdminClient = KustoClientFactory.CreateCslAdminProvider(_kustoConnectionStringBuilder))
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
                    Logger.Error(
                        $"Cannot read database due to {ex}. Possible reason could be database not created or clean %APPDATA%\\Kusto\\tokenCache.data and try again");
                    throw;
                }

                return this._databaseName;
            }
        }
    }
}