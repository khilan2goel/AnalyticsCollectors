using System;
using Kusto.Data;
using Kusto.Ingest;

namespace AnalyticsCollector.KustoService
{
    public class KustoClientFactory: IKustoClientFactory
    {
        private IKustoQueuedIngestClient ingestionClient;
        private string aadTenantIdOrTenantName;
        private string connectionString;

        public KustoClientFactory(string aadTenantIdOrTenantName, string connectionString)
        {
            this.aadTenantIdOrTenantName = aadTenantIdOrTenantName;
            this.connectionString = connectionString;
        }

        public IKustoQueuedIngestClient GetQueuedIngestClient()
        {
            var kcsbDM =
                new KustoConnectionStringBuilder($"https://ingest-{connectionString}").WithAadUserPromptAuthentication(authority: $"{aadTenantIdOrTenantName}");
            return ingestionClient ?? (ingestionClient = KustoIngestFactory.CreateQueuedIngestClient(kcsbDM));
        }

        public KustoConnectionStringBuilder KustoConnectionStringBuilder => new KustoConnectionStringBuilder($"https://{this.connectionString}")
                    .WithAadUserPromptAuthentication(authority: $"{aadTenantIdOrTenantName}");

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            ingestionClient?.Dispose();
        }
    }
}
