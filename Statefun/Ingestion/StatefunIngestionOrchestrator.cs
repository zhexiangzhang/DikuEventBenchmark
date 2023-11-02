using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Common.Ingestion.Config;
using Common.Http;
using Common.Infra;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Statefun.Streaming;

namespace Statefun.Ingestion
{
    public class StatefunIngestionOrchestrator
    {
        private readonly IngestionConfig config;

        private static readonly ILogger logger = LoggerProxy.GetInstance(nameof(StatefunIngestionOrchestrator));

        Dictionary<string, KafkaProducer> kafkaProducers;

        public StatefunIngestionOrchestrator(IngestionConfig config)
		{
			this.config = config;
            if(this.config.concurrencyLevel == 0)
            {
                this.config.concurrencyLevel = Environment.ProcessorCount;
                logger.LogInformation("Set concurrency level of ingestion to processor count: {0}", this.config.concurrencyLevel);
            }

            this.kafkaProducers = new Dictionary<string, KafkaProducer>();
            // initiate kafka producers
            createKafkaProducer();
        }

        public void createKafkaProducer()
        {
            // initiate kafka producers            
            IDictionary<string, string> kafkaIngestTopics = this.config.kafkaIngestTopics;
            if (kafkaIngestTopics == null || kafkaIngestTopics.Count == 0)
            {
                logger.LogError("kafkaIngestTopics is null or empty");
                return;
            }
            List<string> topics = kafkaIngestTopics.Values.ToList();
            foreach (var topic in topics)
            {
                // Console.WriteLine("create kakfa producer for ingestion, Topic " + topic);
                KafkaProducer kafkaProducer = new KafkaProducer(this.config.kafkaService, topic);
                this.kafkaProducers.Add(topic, kafkaProducer);
            }
        }

        public void stopKafkaProducer()
        {
            // stop kafka producers
            foreach (var kafkaProducer in this.kafkaProducers.Values)
            {
                kafkaProducer.Stop();
            }
            logger.LogInformation("Kafka ingest producers stopped");
        }

        public async Task<long> Run(DuckDBConnection connection)
		{
            var startTime = DateTime.UtcNow;

            long totalSubmittedRecords = 0;

            logger.LogInformation("Ingestion process starting at {0} with strategy {1}", startTime, config.strategy.ToString());

            var command = connection.CreateCommand();

            List<Task> tasksToWait = new();

            foreach (var table in config.mapTableToUrl)
            {
                logger.LogInformation("Ingesting table {0} at {1}", table, DateTime.UtcNow);

                command.CommandText = "select * from "+table.Key+";";
                var queryResult = command.ExecuteReader();

                BlockingCollection<JObject> tuples = new BlockingCollection<JObject>();

                Task t1 = Task.Run(() => Produce(tuples, queryResult));

                long rowCount = GetRowCount(queryResult);

                totalSubmittedRecords += rowCount;

                if(rowCount == 0)
                {
                    logger.LogWarning("Table {0} is empty!", table);
                    continue;
                }

                if (config.strategy == IngestionStrategy.TABLE_PER_WORKER)
                {
                    TaskCompletionSource tcs = new TaskCompletionSource();
                    Task t = Task.Run(() => Consume(tuples, table, rowCount, tcs));
                    tasksToWait.Add(tcs.Task);
                }
                else if (config.strategy == IngestionStrategy.WORKER_PER_CPU)
                {

                    for (int i = 0; i < config.concurrencyLevel; i++) {
                        TaskCompletionSource tcs = new TaskCompletionSource();
                        Task t = Task.Run(() => ConsumeShared(tuples, table, rowCount, tcs));
                        tasksToWait.Add(tcs.Task);
                    }
                    await Task.WhenAll(tasksToWait);
                    totalCount = 0;
                    tasksToWait.Clear();
                    logger.LogInformation("Finished loading table {0} at {1}", table, DateTime.UtcNow);
                }
                else // default to single worker
                {
                    TaskCompletionSource tcs = new TaskCompletionSource();
                    Task t = Task.Run(() => Consume(tuples, table, rowCount, tcs));
                    await tcs.Task;
                    logger.LogInformation("Finished loading table {0}", table);
                }
 
            }

            if(tasksToWait.Count > 0)
            {
                await Task.WhenAll(tasksToWait);
                logger.LogInformation("Finished loading all tables");
            }

            stopKafkaProducer();

            TimeSpan span = DateTime.UtcNow - startTime;
            logger.LogInformation("Ingestion process has terminated in {0} seconds", span.TotalSeconds);

            return totalSubmittedRecords;
        }

        private void Produce(BlockingCollection<JObject> tuples, DuckDBDataReader queryResult)
        {
            while (queryResult.Read())
            {
                JObject obj = new JObject();
                for (int ordinal = 0; ordinal < queryResult.FieldCount; ordinal++)
                {
                    var column = queryResult.GetName(ordinal);
                    var val = queryResult.GetValue(ordinal);
                    obj[column] = JToken.FromObject(val);
                }
                tuples.Add(obj);
            }
        }

        private static readonly BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

        private long GetRowCount(DuckDBDataReader queryResult)
        {
            var field = queryResult.GetType().GetField("rowCount", bindingFlags);
            return (long)field?.GetValue(queryResult);
        }

        int totalCount = 0;

        private void ConsumeShared(BlockingCollection<JObject> tuples, KeyValuePair<string,string> entry, long rowCount, TaskCompletionSource tcs)
        {            
            logger.LogDebug("Ingestion worker ID {0} has started", Environment.CurrentManagedThreadId);
            JObject jobject;
            do
            {
                bool taken = tuples.TryTake(out jobject);
                if (taken)
                {
                    Interlocked.Increment(ref totalCount);
                    ConvertAndSendToKafka(jobject, entry);                    
                }
            } while (Volatile.Read(ref totalCount) < rowCount);
            logger.LogDebug("Ingestion worker ID {0} has finished", Environment.CurrentManagedThreadId);
            tcs.SetResult();
        }

        private void Consume(BlockingCollection<JObject> tuples, KeyValuePair<string,string> entry, long rowCount, TaskCompletionSource tcs)
        {
            int currRow = 1;
            do
            {
                JObject obj = tuples.Take();
                ConvertAndSendToKafka(obj, entry);                
                currRow++;
            } while (currRow <= rowCount);
            tcs.SetResult();
        }

        private void ConvertAndSend(JObject obj, KeyValuePair<string,string> entry)
        {
            string url = entry.Value;
            string strObj = JsonConvert.SerializeObject(obj);

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, url);
            message.Content = HttpUtils.BuildPayload(strObj);

            try
            {
                using HttpResponseMessage response = HttpUtils.client.Send(message);
            }
            catch (Exception e)
            {
                logger.LogInformation("Exception message: {0}", e.Message);
            }
        }

        /**
        * For StateFun only
        *   This method is used to convert a payload into the format accepted by the StateFun application
        *   and extract the partitionID from object.
        */
        private (string, string) CreateNewObject(JObject obj, string objectName, string partitionIDKey)
        {
            JObject newObject = new JObject();
            newObject[objectName] = obj;
            string partitionID = obj[partitionIDKey].ToString();
            string serializedObject = JsonConvert.SerializeObject(newObject);
            
            return (serializedObject, partitionID);
        }

        private async void ConvertAndSendToKafka(JObject obj, KeyValuePair<string,string> entry) {
            string keyID = "";
            string finalJson = "";
            int partitionNumber = 0;
            string ingestionEvent = "";

            if (entry.Key == "customers") 
            {
                ingestionEvent = "initCustomer";
                (finalJson, keyID) = CreateNewObject(obj, "customer", "id");
                partitionNumber = config.customerPartion;  
            } 
            else if (entry.Key == "sellers") 
            {
                ingestionEvent = "initSeller";
                (finalJson, keyID) = CreateNewObject(obj, "seller", "id");
                partitionNumber = config.sellerPartion;                            
            } 
            else if (entry.Key == "products") 
            {
                ingestionEvent = "addProducts";
                (finalJson, keyID) = CreateNewObject(obj, "product", "product_id");
                partitionNumber = config.productPartion;                                        
            }
            else if (entry.Key == "stock_items") 
            {
                ingestionEvent = "addStockItems";
                (finalJson, keyID) = CreateNewObject(obj, "stockItem", "product_id");
                partitionNumber = config.stockPartion;                               
            }
            int partitionID = int.Parse(keyID) % partitionNumber;                             
            string topicName = this.config.kafkaIngestTopics[ingestionEvent];    
            await this.kafkaProducers[topicName].ProduceAsync(partitionID.ToString(), finalJson, 0); 

            // Interlocked.Increment(ref totalSubmittedRecords);  // increment totalSubmittedRecords by 1           
        }
    }
}