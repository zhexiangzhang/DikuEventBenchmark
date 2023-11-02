using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http;
using Client.DataGeneration;
using Client.DataGeneration.Real;
using Client.Collection;
using Client.Infra;
using Client.Ingestion;
using Client.Streaming.Kafka;
using Common;
using Common.Http;
using Common.Workload;
using Common.Workload.Customer;
using Common.Entities;
using Common.Streaming;
using DuckDB.NET.Data;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Streams;
using Client.Workload;
using Client.Ingestion.Config;
using Client.Streaming.Redis;
using Common.Infra;
using System.Linq;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;


namespace Client.Workflow
{
	public class WorkflowOrchestrator
	{

        public WorkflowConfig workflowConfig = null;

        public SyntheticDataSourceConfig syntheticDataConfig = null;

        public OlistDataSourceConfiguration olistDataConfig = null;

        public IngestionConfig ingestionConfig = null;

        public WorkloadConfig workloadConfig;    

        public KafkaConfig kafkaConfig = null;

        public CollectionConfig collectionConfig = null;

        // orleans client
        private readonly IClusterClient orleansClient;

        // streams
        private readonly IStreamProvider streamProvider;

        private readonly ILogger logger;

        // synchronization with possible many ingestion orchestrator
        // CountdownEvent ingestionProcess;

        public WorkflowOrchestrator(IClusterClient orleansClient, 
        WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, 
        IngestionConfig ingestionConfig, WorkloadConfig workloadConfig, KafkaConfig kafkaConfig, 
        CollectionConfig collectionConfig)
		{
            this.orleansClient = orleansClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            
            this.workflowConfig = workflowConfig;
            this.syntheticDataConfig = syntheticDataConfig;
            this.ingestionConfig = ingestionConfig;
            this.workloadConfig = workloadConfig;
            this.kafkaConfig = kafkaConfig;
            this.collectionConfig = collectionConfig;

            this.logger = LoggerProxy.GetInstance("WorkflowOrchestrator");
        }

        /**
         * Initialize the first step
         */
        public async Task Run()
        {
            
                using var connection = new DuckDBConnection(syntheticDataConfig.connectionString);
                connection.Open();
            // Console.WriteLine("WorkflowOrchestrator.Run()*******************");


            if (this.workflowConfig.healthCheck)
            {

                string redisConnection = string.Format("{0}:{1}", this.workloadConfig.streamingConfig.host, this.workloadConfig.streamingConfig.port);

                // for each table and associated url, perform a GET request to check if return is OK
                // health check. is the microservice online?
                var responses = new List<Task<HttpResponseMessage>>();
                foreach (var tableUrl in ingestionConfig.mapTableToUrl)
                {
                    var urlHealth = tableUrl.Value + WorkflowConfig.healthCheckEndpoint;
                    logger.LogInformation("Contacting {0} healthcheck on {1}", tableUrl.Key, urlHealth);
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, urlHealth);
                    responses.Add(HttpUtils.client.SendAsync(message));
                }

                try
                {
                    await Task.WhenAll(responses);
                } catch(Exception e)
                {
                    logger.LogError("Error on contacting healthcheck: {0}", e.Message);
                    return;
                }

                int idx = 0;
                foreach (var tableUrl in ingestionConfig.mapTableToUrl)
                {
                    if (!responses[idx].Result.IsSuccessStatusCode)
                    {
                        logger.LogError("Healthcheck failed for {0} in URL {1}", tableUrl.Key, tableUrl.Value);
                        return;
                    }
                    idx++;
                }

                logger.LogInformation("Healthcheck succeeded for all URLs {1}", ingestionConfig.mapTableToUrl);

                // https://stackoverflow.com/questions/27102351/how-do-you-handle-failed-redis-connections
                // https://stackoverflow.com/questions/47348341/servicestack-redis-service-availability
                if (this.workflowConfig.transactionSubmission && !RedisUtils.TestRedisConnection(redisConnection))
                {
                    logger.LogInformation("Healthcheck failed for Redis in URL {0}", redisConnection);
                }

                logger.LogInformation("Healthcheck process succeeded");
            }

            if (this.workflowConfig.dataLoad)
            {
                if(this.syntheticDataConfig != null)
                {
                    var syntheticDataGenerator = new SyntheticDataGenerator(syntheticDataConfig);
                    syntheticDataGenerator.Generate(connection);
                } else {

                    if(this.olistDataConfig == null)
                    {
                        throw new Exception("Loading data is set up but no configuration was found!");
                    }

                    var realDataGenerator = new RealDataGenerator(olistDataConfig);
                    realDataGenerator.Generate();

                }
            }


          

            long recordBeforeIngest = await MetricGather.getCurrentReadRecord();

            long recordIngest = 0;
            long recordAfterIngest = 0;
            if (this.workflowConfig.ingestion)
            {
                var ingestionOrchestrator = new SimpleIngestionOrchestrator(ingestionConfig, kafkaConfig);
                recordIngest = ingestionOrchestrator.Run(connection);
                // ingestionOrchestrator.Run(connection);
            }

            recordAfterIngest = recordBeforeIngest + recordIngest;

            Console.WriteLine("record beforeIngest {0}, recordIngest {1}, recordAfterIngest {2}", recordBeforeIngest, recordIngest, recordAfterIngest);

            // before start next step, need to make sure statefun have read all data            
            while (this.workflowConfig.ingestion) {
                long currentRecord = await MetricGather.getCurrentReadRecord();
                Console.WriteLine("Waiting for read all data, {0}/{1}", currentRecord, recordAfterIngest);
                await Task.Delay(500);
                if (currentRecord == recordAfterIngest) {
                    Console.WriteLine("All data have been read, ready to generate transaction");                    
                    break;
                }
            }
            
            if (this.workflowConfig.transactionSubmission)
            {
                // // 等待秒
                // await Task.Delay(3000);
            
                string redisConnection = string.Format("{0}:{1}", this.workloadConfig.streamingConfig.host, this.workloadConfig.streamingConfig.port);

                // get number of products
                
                var endValue = DuckDbUtils.Count(connection, "products");
                if (endValue < this.workloadConfig.customerWorkerConfig.maxNumberKeysToBrowse || endValue < this.workloadConfig.customerWorkerConfig.maxNumberKeysToAddToCart)
                {
                    throw new Exception("Number of available products < possible number of keys to checkout. That may lead to customer grain looping forever!");
                }

                // update customer config
                long numSellers = DuckDbUtils.Count(connection, "sellers");
                // defined dynamically
                this.workloadConfig.customerWorkerConfig.sellerRange = new Interval(1, (int)numSellers);

                // activate all customer workers
                List<Task> tasks = new();
                List<Customer> customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
                ICustomerWorker customerWorker = null;
                foreach (var customer in customers)
                {
                    customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(customer.id);
                    tasks.Add( customerWorker.Init(this.workloadConfig.customerWorkerConfig, customer, this.workloadConfig.endToEndLatencyCollection, redisConnection) );
                }
                await Task.WhenAll(tasks);

                // make sure to activate all sellers so they can respond to customers when required
                // another solution is making them read from the microservice itself...
                ISellerWorker sellerWorker = null;
                tasks.Clear();
                for (int i = 1; i <= numSellers; i++)
                {
                    List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
                    sellerWorker = this.orleansClient.GetGrain<ISellerWorker>(i);
                    tasks.Add(sellerWorker.Init(this.workloadConfig.sellerWorkerConfig, products, this.workloadConfig.endToEndLatencyCollection, redisConnection));
                }
                await Task.WhenAll(tasks);

                // could be read in the data load config, but in cases the file is not read, reading from DB ensures the value is always fulfilled
                long numCustomers = DuckDbUtils.Count(connection, "customers");
                var customerRange = new Interval(1, (int)numCustomers);

                // activate delivery worker
                await this.orleansClient.GetGrain<IDeliveryWorker>(0).Init(this.workloadConfig.deliveryWorkerConfig);
        
                // todo
                // *********************************************            
                List<KafkaConsumer> kafkaWorkers = new();
                List<Task> kafkaTasks = new List<Task>();
                if (this.kafkaConfig.KafkaEnabled)
                {                                       
                    foreach (var entry in this.workloadConfig.mapTopicToStreamGuid)
                    {
                        KafkaConsumer kafkaConsumer = new KafkaConsumer(
                            KafkaUtils.BuildKafkaConsumer(entry.Key, this.kafkaConfig.KafkaService),
                            this.orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider),
                            entry.Value,
                            entry.Key);
                        kafkaTasks.Add(Task.Factory.StartNew(() => kafkaConsumer.Run(), TaskCreationOptions.LongRunning));                                               
                        kafkaWorkers.Add(kafkaConsumer);
                    }                   
                }
                // *********************************************
                
                // setup transaction orchestrator
                WorkloadOrchestrator workloadOrchestrator = new WorkloadOrchestrator(this.orleansClient, this.workloadConfig, customerRange);
                Console.WriteLine("=======================================");
                var workloadTask = Task.Run(workloadOrchestrator.Run);
                                
                // listen for cluster client disconnect and stop the sleep if necessary... Task.WhenAny...
                await Task.WhenAny(workloadTask, OrleansClientFactory._siloFailedTask.Task);
                foreach(var kafkaWorker in kafkaWorkers)
                {   
                    kafkaWorker.Stop();
                }                
                
                Console.WriteLine("Kafka consumers stopped");
                await Task.WhenAll(kafkaTasks);

                DateTime startTime = workloadTask.Result.startTime;
                DateTime finishTime = workloadTask.Result.finishTime;                

                Console.WriteLine("========================================");

                if (this.workflowConfig.collection)
                {
                    MetricGather metricGather = new MetricGather(orleansClient, customers, numSellers, collectionConfig);
                    await metricGather.Collect(startTime, finishTime);
                }

            }

            if (this.workflowConfig.cleanup)
            {
                // DuckDbUtils.DeleteAll(connection, "products", "seller_id = " + i);
            }

            return;

        }        

    }
}