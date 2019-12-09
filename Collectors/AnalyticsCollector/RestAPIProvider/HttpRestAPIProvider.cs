using System.Collections.Generic;
using System.Linq;
using System.Net.Http;

namespace AnalyticsCollector
{
    public class HttpRestAPIProvider
    {
        public HttpClient httpClient;

        public HttpRestAPIProvider(HttpClient httpClient)
        {
            this.httpClient = httpClient;
        }


        public string GetHeaderValue(HttpResponseMessage response, string headerName)
        {
            if (response == null)
            {
                return string.Empty;
            }

            IEnumerable<string> headerValue;
            if (!response.Headers.TryGetValues(headerName, out headerValue))
            {
                if (response.Content != null)
                {
                    response.Content.Headers.TryGetValues(headerName, out headerValue);
                }
            }
            return headerValue?.FirstOrDefault();
        }
    }
}