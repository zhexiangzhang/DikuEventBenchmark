using Common.Infra;
using Common.Entities;
using Common.Experiment;
using Common.Workload;
using Common.Services;
using Common.Workers;
using Statefun.Workload;
using Statefun.Metric;
using Statefun.Workers;
using Statefun.Services;
using Newtonsoft.Json.Linq;
using Statefun.Ingestion;
using DuckDB.NET.Data;
using Statefun.Streaming;
using Statefun.Streaming.KafkaUtil;
using Confluent.Kafka;

namespace Statefun;

public class StatefunExperimentManager : ExperimentManager
{      
    private readonly StatefunSellerService sellerService;
    private readonly StatefunCustomerService customerService;
    private readonly StatefunDeliveryService deliveryService;

    private readonly Dictionary<int, StatefunSellerThread> sellerThreads;
    private readonly Dictionary<int, StatefunCustomerThread> customerThreads;
    private readonly StatefunDeliveryThread deliveryThread;

    private List<KafkaConsumer> kafkaWorkers;

    private Dictionary<string, KafkaProducerThread> kafkaProducerThreads;

    private int numSellers;

    private StatefunWorkloadManager workloadManager;
    private readonly StatefunMetricManager metricManager;

    private CancellationTokenSource cancellationTokenSource;

    public StatefunExperimentManager(ExperimentConfig config) : base(config)
    {               
        this.deliveryThread = StatefunDeliveryThread.BuildDeliveryThread(config.deliveryWorkerConfig);
        this.deliveryService = new StatefunDeliveryService(this.deliveryThread);

        this.sellerThreads = new Dictionary<int, StatefunSellerThread>();
        this.sellerService = new StatefunSellerService(this.sellerThreads);
        this.customerThreads = new Dictionary<int, StatefunCustomerThread>();
        this.customerService = new StatefunCustomerService(this.customerThreads);

        this.numSellers = 0;
        
        this.metricManager = new StatefunMetricManager(sellerService, customerService, deliveryService);

        this.kafkaWorkers = new List<KafkaConsumer>();
        this.kafkaProducerThreads = new Dictionary<string, KafkaProducerThread>();
    }

    protected override WorkloadManager SetUpManager(int runIdx)
    {
        this.workloadManager ??= new StatefunWorkloadManager(
            sellerService, customerService, deliveryService,
            config.transactionDistribution,
            customerRange,
            config.concurrencyLevel,
            config.executionTime,
            config.delayBetweenRequests);            
        this.workloadManager.SetUp(config.runs[runIdx].sellerDistribution, new Interval(1, this.numSellers));
        return workloadManager;
    }

    protected override void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        string ts = new DateTimeOffset(startTime).ToUnixTimeMilliseconds().ToString();
        this.metricManager.SetUp(numSellers, config.numCustomers);
        this.metricManager.Collect(startTime, finishTime, config.epoch, string.Format("{0}#{1}_{2}_{3}_{4}_{5}", ts, runIdx, config.concurrencyLevel,
                    config.runs[runIdx].numProducts, config.runs[runIdx].sellerDistribution, config.runs[runIdx].keyDistribution));
    }

    protected override void PostExperiment()
    {
        Console.WriteLine("Post Experiment");
    }

    protected override void PostRunTasks(int runIdx, int lastRunIdx)
    {
        
        logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.UtcNow);

        logger.LogInformation("Memory used before collection:       {0:N0}",
                GC.GetTotalMemory(false));

        // Collect all generations of memory.
        GC.Collect();
        logger.LogInformation("Memory used after full collection:   {0:N0}",
        GC.GetTotalMemory(true));
    }

    // todo :  * 3. Initialize delivery as a single object, but multithreaded ???
    protected override void PreExperiment()
    {
        // initialize all customer thread objects
        Console.WriteLine("0000 Done");     
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerThreads.Add(i, StatefunCustomerThread.BuildCustomerThread(sellerService, config.numProdPerSeller, config.customerWorkerConfig, customers[i-1]));
        }
        Console.WriteLine("1111 Done");                        
    }

    /**
     * Initialize seller threads   
     * Set up distribution for each customer thread  
     * Initialize kafka consumer for transaction mark
     * Initialize kafkaProducer threads
     */
    protected override void PreWorkload(int runIdx)
    {
        this.numSellers = (int)DuckDbUtils.Count(connection, "sellers");
         
        for (int i = 1; i <= numSellers; i++)
        {
            List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
            if (!sellerThreads.ContainsKey(i))
            {
                sellerThreads[i] = StatefunSellerThread.BuildSellerThread(i, config.sellerWorkerConfig);
                sellerThreads[i].SetUp(products, config.runs[runIdx].keyDistribution);
            }
            else
            {
                sellerThreads[i].SetUp(products, config.runs[runIdx].keyDistribution);
            }
        }
        Console.WriteLine("222 Done");      
        Interval sellerRange = new Interval(1, this.numSellers);
        for (int i = customerRange.min; i <= customerRange.max; i++)
        {
            this.customerThreads[i].SetDistribution(this.config.runs[runIdx].sellerDistribution, sellerRange, this.config.runs[runIdx].keyDistribution);
        }
        
        // initialize kafka consumer
        // List<KafkaConsumer> kafkaWorkers = new();
        
        List<Task> kafkaTasks = new List<Task>();                 
        cancellationTokenSource = new CancellationTokenSource();                      
        foreach (var topic in this.config.kafkaConsumerTopics)
        {
            StatefunSellerService sellerService_ = null;
            StatefunCustomerService customerService_ = null;
            StatefunDeliveryService deliveryService_ = null;

            switch (topic)
            {
                case "updateProductTask":
                case "updatePriceTask":
                case "queryDashboardTask":
                    sellerService_ = this.sellerService;
                    break;
                case "checkoutTask":
                    customerService_ = this.customerService;
                    break;
                case "updateDeliveryTask":
                    deliveryService_ = this.deliveryService;
                    break;
                default:
                    throw new Exception("Unknown topic: " + topic);
            }

            KafkaConsumer kafkaConsumer = new KafkaConsumer(
                ConsumerBuilder.BuildKafkaConsumer(topic, this.config.kafkaService),
                topic, sellerService_, customerService_, deliveryService_);
                
            kafkaTasks.Add(Task.Factory.StartNew(() => kafkaConsumer.Run(cancellationTokenSource.Token), TaskCreationOptions.LongRunning));                                               
            this.kafkaWorkers.Add(kafkaConsumer);
        }  
    
        // initialize kafka producer threads
        string[] kafkaProducerTopics = config.kafkaProducerTopics;
        foreach (var topic in kafkaProducerTopics)
        {
            // Console.WriteLine("Initialize kafka transaction producer thread for topic: " + topic + " " + config.kafkaService);
            KafkaProducerThread kafkaProducerThread = KafkaProducerThread.BuildKafkaProducerThread(config.kafkaService, topic);
            kafkaProducerThreads.Add(topic, kafkaProducerThread);
            kafkaProducerThread.Start();
        }
    }

    /*
        before + ingest = after
    */
    protected override async Task RunIngestion(DuckDBConnection connection)
    {
        long beforeReadRecord = await getCurrentReadRecord();
        var ingestionOrchestrator = new StatefunIngestionOrchestrator(config.ingestionConfig);
        long ingestRecord = await ingestionOrchestrator.Run(connection);

        long expectReadRecord = ingestRecord + beforeReadRecord;
        while (true) {        
            long afterReadRecord = await getCurrentReadRecord();
            Console.WriteLine("Waiting for read all data, {0}/{1}", afterReadRecord, expectReadRecord);                
            await Task.Delay(500);
            if (afterReadRecord == expectReadRecord) {
                Console.WriteLine("All data have been read, ready to generate transaction");                    
                break;
            }
        } 
        Console.WriteLine("return ?????????????????????");           
    }


    // protected override async Task RunIngestion(DuckDBConnection connection)
    // {
    //     // long beforeReadRecord = await getCurrentReadRecord();
    //     var ingestionOrchestrator = new StatefunIngestionOrchestrator(config.ingestionConfig);
    //     long ingestRecord = await ingestionOrchestrator.Run(connection);

    //     // long expectReadRecord = ingestRecord + beforeReadRecord;
    //     // while (true) {        
    //     //     long afterReadRecord = await getCurrentReadRecord();
    //     //     Console.WriteLine("Waiting for read all data, {0}/{1}", afterReadRecord, expectReadRecord);                
    //     //     await Task.Delay(500);
    //     //     if (afterReadRecord == expectReadRecord) {
    //     //         Console.WriteLine("All data have been read, ready to generate transaction");                    
    //     //         break;
    //     //     }
    //     // }   

    //     await Task.Delay(500);      
    // }

    protected override void TrimStreams()
    {                
        foreach (var kafkaConsumer in this.kafkaWorkers)
        {
            kafkaConsumer.Dispose();
        }
        cancellationTokenSource.Cancel();
        Console.WriteLine("Close all the consumers");
    }

     /*
    * For statefun :
    *   record received by statefun
    */
    public async Task<long> getCurrentReadRecord()
    {
        using (HttpClient client = new HttpClient())
        {
            try
            {                                        
                HttpResponseMessage response = await client.GetAsync(this.config.stateFunUrl + "jobs");
                response.EnsureSuccessStatusCode();
                string responseBody = await response.Content.ReadAsStringAsync();                
                JObject responseJson = JObject.Parse(responseBody);                
                string jobId = (string)responseJson["jobs"][0]["id"];
                                    
                string newUrl = this.config.stateFunUrl + "jobs/" + jobId;
                response = await client.GetAsync(newUrl);
                response.EnsureSuccessStatusCode();
                responseBody = await response.Content.ReadAsStringAsync();
                responseJson = JObject.Parse(responseBody);                    
                                
                JObject targetVertex = responseJson["vertices"].FirstOrDefault(v => (string)v["name"] == "feedback-union -> functions -> Sink: e-commerce.fns-kafkaSink-egress") as JObject;

                // JObject targetVertex = responseJson["vertices"].FirstOrDefault(v => (string)v["name"] == "feedback-union -> functions -> (Sink: e-commerce.fns-kafkaSinkShipmentUpd-egress, Sink: e-commerce.fns-kafkaSinkSeller-egress, Sink: e-commerce.fns-kafkaSinkCheckout-egress)") as JObject;

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
}


