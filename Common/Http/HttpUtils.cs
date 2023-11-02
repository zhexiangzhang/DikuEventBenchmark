using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Common.Http
{
    public sealed class HttpUtils
    {

        // https://learn.microsoft.com/en-us/dotnet/api/system.net.http.httpclient?view=net-7.0
        // // HttpClient must be instantiated once per application. See above
        public static readonly HttpClient client = new HttpClient();

        private static readonly string httpJsonContentType = "application/json";

        private static readonly Encoding encoding = Encoding.UTF8;

        public static StringContent BuildPayload(string item)
        {
            return new StringContent(item, encoding, httpJsonContentType);
        }

        /**
        * For StateFun only
        *   used to send http request to StateFun application.
        */
        public static async Task SendHttpToStatefun(string url, string contentType, string payLoad)
        {
            var content = HttpUtils.BuildPayload(payLoad);
            content.Headers.ContentType = null; // zero out default content type
            content.Headers.TryAddWithoutValidation("Content-Type", contentType);
            var response = await HttpUtils.client.PostAsync(url, content);
                    
            // Console.WriteLine("Status Code: " + (int)response.StatusCode);
            // string responseContent = await response.Content.ReadAsStringAsync();
            // Console.WriteLine("Response: " + responseContent);
        }
    }
}