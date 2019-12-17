using System;
using Kusto.Data;
using Kusto.Ingest;

namespace AnalyticsCollector.KustoService
{
    public interface IKustoClientFactory : IDisposable
    {
        IKustoQueuedIngestClient GetQueuedIngestClient();

        KustoConnectionStringBuilder KustoConnectionStringBuilder { get; }
    }
}
