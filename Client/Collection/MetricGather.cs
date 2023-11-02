using System;
using Common.Entities;
using Common.Http;
using Common.Workload.Metrics;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Orleans;
using Newtonsoft.Json;
using System.Dynamic;
using System.Linq;
using Common.Infra;
using Common.Workload;
using System.IO;
using System.Text;
using System.Linq;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Client.Collection
{
	public class MetricGather
	{
        private readonly IClusterClient orleansClient;
        private readonly List<Customer> customers;
        private readonly long numSellers;
        private readonly CollectionConfig collectionConfig;
        private readonly ILogger logger;

        public MetricGather(IClusterClient orleansClient, List<Customer> customers, long numSellers,
            CollectionConfig collectionConfig)
		{
            this.orleansClient = orleansClient;
            this.customers = customers;
            this.numSellers = numSellers;
            this.collectionConfig = collectionConfig;
            this.logger = LoggerProxy.GetInstance("MetricGather");
        }

		public async Task Collect(DateTime startTime, DateTime finishTime)
		{

            StreamWriter sw = new StreamWriter(string.Format("results_{0}_{1}.txt", startTime.Millisecond, finishTime.Millisecond));

            sw.WriteLine("Run from {0} to {1}", startTime, finishTime);
            sw.WriteLine("===========================================");

            Console.WriteLine(startTime.Millisecond + " ^^^^ " + finishTime.Millisecond);
            // collect() stops subscription to redis streams in every worker

            var latencyGatherTasks = new List<Task<List<Latency>>>();

            foreach (var customer in customers)
            {
                var customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(customer.id);
                latencyGatherTasks.Add(customerWorker.Collect(startTime));
            }

            for (int i = 1; i <= numSellers; i++)
            {
                var sellerWorker = this.orleansClient.GetGrain<ISellerWorker>(i);
                latencyGatherTasks.Add(sellerWorker.Collect(startTime));
            }

            latencyGatherTasks.Add(this.orleansClient.GetGrain<IDeliveryWorker>(0).Collect(startTime));

            await Task.WhenAll(latencyGatherTasks);

            Dictionary<TransactionType, List<double>> latencyCollPerTxType = new Dictionary<TransactionType, List<double>>();

            var txTypeValues = Enum.GetValues(typeof(TransactionType)).Cast<TransactionType>().ToList();
            foreach(var txType in txTypeValues)
            {
                latencyCollPerTxType.Add(txType, new List<double>());
            }

            int maxTid = 0;                
            int totalTransactions = 0;
            double totalLatency = 0.0;
            foreach (var list in latencyGatherTasks)
            {
                foreach(var entry in list.Result)
                {
                    latencyCollPerTxType[entry.type].Add(entry.period);
                    if (entry.tid > maxTid) maxTid = entry.tid;

                    totalTransactions++;
                    totalLatency += entry.period;
                }
            }

            // Compute the average latency for all transactions
            double averageLatency = totalLatency / totalTransactions;

            // throughput
            // getting the Shared.Workload.Take().tid - 1 does not mean the system has finished processing it
            // therefore, we need to get the last (i.e., maximum) tid processed from the grains

            // transactions per second
            TimeSpan timeSpan = finishTime - startTime;
            // Console.WriteLine(startTime.Millisecond + " *** " + finishTime.Millisecond);
            int secondsTotal = ((timeSpan.Minutes * 60) + timeSpan.Seconds);
            // Console.WriteLine(timeSpan.Minutes + " ***2 " + timeSpan.Seconds);
            // Console.WriteLine(secondsTotal + " ***3 ");
            decimal txPerSecond = decimal.Divide(maxTid , secondsTotal);

            logger.LogInformation("Number of seconds: {0}", secondsTotal);
            sw.WriteLine("Number of seconds: {0}", secondsTotal);
            logger.LogInformation("Number of completed transactions: {0}", maxTid);
            sw.WriteLine("Number of completed transactions: {0}", maxTid);
            logger.LogInformation("Transactions per second: {0}", txPerSecond);
            sw.WriteLine("Transactions per second: {0}", txPerSecond);
            sw.WriteLine("===========================================");
            
           // Logging the latency for each type of transaction
            // foreach (var txType in txTypeValues)
            // {
            //     logger.LogInformation("transactions type {0}", txType);
            //     // 判断数组是否存在这个txType类型的事务，可能没有，直接访问会报错
            //     if (!latencyCollPerTxType.ContainsKey(txType) ) continue;
                

            //     double avgLatencyForTxType = latencyCollPerTxType[].Average();
            //     logger.LogInformation("Average latency for {0}: {1}ms", txType, avgLatencyForTxType);
            //     sw.WriteLine("Average latency for {0}: {1}ms", txType, avgLatencyForTxType);
            // }

            // Logging the average latency for all transactions
            logger.LogInformation("Average latency for all transactions: {0}ms", averageLatency);
            sw.WriteLine("Average latency for all transactions: {0}ms", averageLatency);
            sw.WriteLine("===========================================");
            
            // end_of_file:

            sw.WriteLine("===========    THE END   ==============");
            sw.Flush();
            sw.Close();
        }

        private static long GetCount(string respStr)
        {
            dynamic respObj = JsonConvert.DeserializeObject<ExpandoObject>(respStr);

            if (respObj.data.result.Count == 0) return 0;

            ExpandoObject data = respObj.data.result[0];
            string count = (string)((List<object>)data.ElementAt(1).Value)[1];
            return Int64.Parse(count);
        }


        /*
        * For statefun :
        *   record received by statefun
        */

        public static async Task<long> getCurrentReadRecord()
        {
            using (HttpClient client = new HttpClient())
            {
                try
                {                                        
                    HttpResponseMessage response = await client.GetAsync("http://localhost:8081/jobs");                
                    response.EnsureSuccessStatusCode();
                    string responseBody = await response.Content.ReadAsStringAsync();                
                    JObject responseJson = JObject.Parse(responseBody);                
                    string jobId = (string)responseJson["jobs"][0]["id"];
                                        
                    string newUrl = "http://localhost:8081/jobs/" + jobId;
                    response = await client.GetAsync(newUrl);
                    response.EnsureSuccessStatusCode();
                    responseBody = await response.Content.ReadAsStringAsync();
                    responseJson = JObject.Parse(responseBody);                    

                    JObject targetVertex = responseJson["vertices"].FirstOrDefault(v => (string)v["name"] == "feedback-union -> functions -> Sink: e-commerce.fns-kafkaSink-egress") as JObject;
                    long readRecords = (long)targetVertex["metrics"]["read-records"];
                    Console.WriteLine(readRecords);
                    return readRecords;                    
                }
                catch (HttpRequestException ex)
                {                    
                    Console.WriteLine($"Exception: {ex.Message}");
                }
                return 0;
            }
        }


        /**
         * Made it like this to allow for quick tests in main()
         */
        public static async Task<(Dictionary<string, long> ingressCountPerMs, Dictionary<string, long> egressCountPerMs)>
            GetFromPrometheus(CollectionConfig collectionConfig, string time)
        {
            // collect data from prometheus. some examples below:
            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_ingress_count
            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count&name=ReserveInventory
            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count{app_id="cart"}&time=1688136844
            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count{app_id="cart",topic="ReserveStock"}&time=1688136844

          

            var ingressCountPerMs = new Dictionary<string, long>();
            HttpRequestMessage message;
            HttpResponseMessage resp;
            string respStr;
            string query;
            long count;
            foreach (var entry in collectionConfig.ingress_topics)
            {
                // query = string.Format("{app_id=\"{0}\",topic=\"{1}\"}&time={2}", entry.app_id, entry.topic, unixTimeMilliSeconds);
                query = new StringBuilder("{app_id='").Append(entry.app_id).Append("',topic='").Append(entry.topic).Append("'}").Append("&time=").Append
                    (time).ToString();
                message = new HttpRequestMessage(HttpMethod.Get, collectionConfig.baseUrl + "/" + collectionConfig.ingress_count + query);
                resp = HttpUtils.client.Send(message);
                respStr = await resp.Content.ReadAsStringAsync();
                count = GetCount(respStr);

                if (ingressCountPerMs.ContainsKey(entry.app_id))
                {
                    var curr = ingressCountPerMs[entry.app_id];
                    ingressCountPerMs[entry.app_id] = curr + count;
                }
                else
                {
                    ingressCountPerMs.Add(entry.app_id, count);
                }
            }

            var egressCountPerMs = new Dictionary<string, long>();
            foreach (var entry in collectionConfig.egress_topics)
            {
                // query = string.Format("{app_id=\"{0}\",topic=\"{1}\"}&time={2}", entry.app_id, entry.topic, unixTimeMilliSeconds);
                query = new StringBuilder("{app_id='").Append(entry.app_id).Append("',topic='").Append(entry.topic).Append("'}").Append("&time=").Append
                       (time).ToString();
                message = new HttpRequestMessage(HttpMethod.Get, collectionConfig.baseUrl + "/" + collectionConfig.egress_count + query);
                resp = HttpUtils.client.Send(message);
                respStr = await resp.Content.ReadAsStringAsync();
                count = GetCount(respStr);

                if (egressCountPerMs.ContainsKey(entry.app_id))
                {
                    var curr = egressCountPerMs[entry.app_id];
                    egressCountPerMs[entry.app_id] = curr + count;
                }
                else
                {
                    egressCountPerMs.Add(entry.app_id, count);
                }
            }

            return (ingressCountPerMs, egressCountPerMs);

        }

    }
}