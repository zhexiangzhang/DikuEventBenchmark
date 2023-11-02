using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Linq; 
using Client.Ingestion.Config;
using Common.Http;
using DuckDB.NET.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Client.Streaming.Kafka;
using System.Text;
using static DuckDB.NET.NativeMethods;

namespace Client.Ingestion
{
	public class SimpleIngestionOrchestrator
	{

		private readonly IngestionConfig config;
        private readonly BlockingCollection<JObject> tuples;  
        private KafkaConfig kafkaConfig;
        Dictionary<string, KafkaProducer> kafkaProducers;


        public SimpleIngestionOrchestrator(IngestionConfig config, KafkaConfig kafkaConfig)
		{
			this.config = config;
            this.tuples = new BlockingCollection<JObject>();
            this.kafkaProducers = new Dictionary<string, KafkaProducer>();
            this.kafkaConfig = kafkaConfig;

            IDictionary<string, string> ingestTopics = kafkaConfig.ingestTopics;
            List<string> topics = ingestTopics.Values.ToList();
                        
            foreach (var topic in topics)
            {
                Console.WriteLine("Topic " + topic);
                KafkaProducer kafkaProducer = new KafkaProducer(kafkaConfig.KafkaService, topic);
                this.kafkaProducers.Add(topic, kafkaProducer);
            }
        }

		public long Run(DuckDBConnection duckDBConnection)
		{
            Console.WriteLine("Ingestion process is about to start.");

           
                var command = duckDBConnection.CreateCommand();
                
                long readRecord = 0;
                foreach (var table in config.mapTableToUrl)
                {                    
                    Console.WriteLine("Ingesting table {0}", table);

                    command.CommandText = "select * from "+table.Key+";";
                    var queryResult = command.ExecuteReader();

                    Task t1 = Task.Run(() => Produce(queryResult));

                    long rowCount = GetRowCount(queryResult);

                    Task t2 = Task.Run(() => Consume(table, rowCount));

                    Task.WaitAll(t1, t2);

                    readRecord += rowCount;

                    Console.WriteLine("Finished loading table {0}", table);                    
                }
            
            foreach (var kafkaProducer in this.kafkaProducers.Values)
            {
                kafkaProducer.Stop();
            }
            Console.WriteLine("Ingestion process has terminated.");
            return readRecord;
        }

        // private static readonly JsonSerializerSettings settings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };
     

        private void Produce(DuckDBDataReader queryResult)
        {
            
            while (queryResult.Read())
            {
                JObject obj = new JObject();
                for (int ordinal = 0; ordinal < queryResult.FieldCount; ordinal++)
                {
                    var column = queryResult.GetName(ordinal);
                    var val = queryResult.GetValue(ordinal);
                    obj[column] = JToken.FromObject(val);
                    // Console.WriteLine("column: {0}, val: {1}", column, val);
                }                
                this.tuples.Add(obj);
            }

        }

        private static readonly BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

        private long GetRowCount(DuckDBDataReader queryResult)
        {
            var field = queryResult.GetType().GetField("rowCount", bindingFlags);
            return (long)field?.GetValue(queryResult);
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

        private async Task Consume(KeyValuePair<string,string> entry, long rowCount) {
            int currRow = 1;
            JObject obj = null;
            do
            {
                obj = this.tuples.Take();                
                string strObj = JsonConvert.SerializeObject(obj);            
                
                try
                {
                    if(config.targetPlatform == TargetPlatform.STATEFUN){                                                            
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
                        Console.WriteLine("keyID: {0}, eventType: {1}", keyID, ingestionEvent);   
                        int partitionID = int.Parse(keyID) % partitionNumber;     
                        // Console.WriteLine("Thhopic");                       
                        string topicName = kafkaConfig.ingestTopics[ingestionEvent];
                        // Console.WriteLine("Topic" + topicName);
                        await this.kafkaProducers[topicName].ProduceAsync(partitionID.ToString(), finalJson);                             
                    }
                    else 
                    {
                        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, entry.Value);
                        // message.Content = HttpUtils.BuildPayload(obj);
                        using HttpResponseMessage response = HttpUtils.client.Send(message);
                    }                    
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception message: {0}", e.Message);
                }

                currRow++;
            } while (currRow <= rowCount);

        }

	}
}

