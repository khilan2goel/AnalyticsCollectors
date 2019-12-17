using System;
using Kusto.Ingest;

namespace AnalyticsCollector.KustoService
{
    public interface IKustoClientFactory : IDisposable
    {
        IKustoQueuedIngestClient GetQueuedIngestClient();
    }
}
