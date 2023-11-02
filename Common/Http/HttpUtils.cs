﻿using System.Text;

namespace Common.Http
{
    public sealed class HttpUtils
    {

        // https://www.stevejgordon.co.uk/using-httpcompletionoption-responseheadersread-to-improve-httpclient-performance-dotnet
        // https://www.stevejgordon.co.uk/httpclient-connection-pooling-in-dotnet-core
        public static readonly HttpClient client = new HttpClient(new SocketsHttpHandler()
        {
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(10),
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            UseProxy = false,
            Proxy = null
        });

        private static readonly string httpJsonContentType = "application/json";

        private static readonly Encoding encoding = Encoding.UTF8;

        public static StringContent BuildPayload(string item)
        {
            return new StringContent(item, encoding, httpJsonContentType);
        }
        
    }
}