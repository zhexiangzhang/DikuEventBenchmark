using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Distribution;
using Common.Infra;
using Common.Entities;
using Common.Workers;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Common.Requests;
using MathNet.Numerics.Distributions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Statefun.Workload;
using Statefun.Streaming;
using Newtonsoft.Json.Linq;

namespace Statefun.Workers
{
    public class StatefunSellerThread : ISellerWorker
    {
        private readonly Random random;
        private readonly SellerWorkerConfig config;

        private int sellerId;

        private IDiscreteDistribution productIdGenerator;

        private readonly HttpClient httpClient;

        private readonly ILogger logger;

        private Product[] products;

        private Random randomKafka;

        private readonly List<TransactionIdentifier> submittedTransactions;
        private readonly List<TransactionOutput> finishedTransactions;

        // private KafkaProducer kafkaProducer_priceU;
        // private KafkaProducer kafkaProducer_proU;
        // private KafkaProducer kafkaProducer_query;

        public static StatefunSellerThread BuildSellerThread(int sellerId, SellerWorkerConfig workerConfig)
        {
            var logger = LoggerProxy.GetInstance("SellerThread_"+ sellerId);            
            return new StatefunSellerThread(sellerId, workerConfig, logger);
        }

        private StatefunSellerThread(int sellerId, SellerWorkerConfig workerConfig, ILogger logger)
        {
            this.random = Random.Shared;
            this.logger = logger;
            this.submittedTransactions = new List<TransactionIdentifier>();
            this.finishedTransactions = new List<TransactionOutput>();
            this.sellerId = sellerId;            
            this.config = workerConfig;
            
            this.randomKafka = new Random();
            // this.kafkaProducer_proU = new KafkaProducer("kafkahost:9092", "productUpdate");
            // this.kafkaProducer_priceU = new KafkaProducer("kafkahost:9092", "priceUpdate");
            // this.kafkaProducer_query = new KafkaProducer("kafkahost:9092", "queryDashboard");
        }

        public void SetUp(List<Product> products, DistributionType keyDistribution)
        {
            this.products = products.ToArray();
            this.productIdGenerator = keyDistribution == DistributionType.UNIFORM ?
                                    new DiscreteUniform(1, products.Count, Random.Shared) :
                                    new Zipf(0.99, products.Count, Random.Shared);
        }
        
        public void Run(int tid, TransactionType type)
        {            
            switch (type)
            {
                case TransactionType.QUERY_DASHBOARD:
                {
                    BrowseDashboard(tid);
                    break;
                }
                case TransactionType.UPDATE_PRODUCT:
                {
                    UpdateProduct(tid);
                    break;
                }
                case TransactionType.PRICE_UPDATE:
                {   
                    UpdatePrice(tid);
                    break;
                }
            }
        }

        public async void BrowseDashboard(int tid)
        {
            long partitionID = this.sellerId;

            JObject payloadObject = new JObject();
            payloadObject["tid"] = tid;
            string payload = JsonConvert.SerializeObject(payloadObject);    
            
            // Console.WriteLine("Seller Thread" + this.sellerId + "BrowseDashboard sent to queue");

            Console.WriteLine("[Send]  [Tid: {0}]  [Seller: {1}]  [BrowseDashboard]", tid, this.sellerId); 
            // Console.WriteLine(" (++) (Tid - " + tid + ") Seller " + this.sellerId + " BrowseDashboard"); 

            // int partitionkafka = randomKafka.Next(10);       
            int partitionkafka = 0;

            await StatefunShared.QueryDashboardMessages.Writer.WriteAsync(
                new KafkaTransactionMessage(
                        partitionID.ToString(),
                        payload,
                        partitionkafka
            ));  
            // await this.kafkaProducer_query.ProduceAsync(partitionID.ToString(), payload);

            // Console.WriteLine("Seller Thread " + this.sellerId + " WriteAsync to queue return"); 
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.QUERY_DASHBOARD, DateTime.UtcNow));
        }
        
        public List<TransactionOutput> GetFinishedTransactions()
        {
            return this.finishedTransactions;
        }

        public Product GetProduct(int idx)
        {
            return this.products[idx];
        }

        public List<TransactionIdentifier> GetSubmittedTransactions()
        {
            return this.submittedTransactions;
        }

        public void AddFinishedTransaction(TransactionOutput transactionOutput)
        {
            this.finishedTransactions.Add(transactionOutput);
        }

        public async void UpdatePrice(int tid)
        {
            int idx = this.productIdGenerator.Sample() - 1;
            object locked = products[idx];

            while(!Monitor.TryEnter(locked))
            {
                idx = this.productIdGenerator.Sample() - 1;
                locked = products[idx];
            }

            var productToUpdate = products[idx];

            int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);
            var currPrice = productToUpdate.price;
            var newPrice = currPrice + ((currPrice * percToAdjust) / 100);
            
            
            string serializedObject = JsonConvert.SerializeObject(new PriceUpdate(this.sellerId, productToUpdate.product_id, newPrice, tid));
            int partitionID = (int)(productToUpdate.product_id) % config.productPartion;

            // int partitionkafka = randomKafka.Next(10);  
            int partitionkafka = 0;

            try{           
                await StatefunShared.PriceUpdateMessages.Writer.WriteAsync(
                    new KafkaTransactionMessage(
                            partitionID.ToString(),
                            serializedObject,
                            partitionkafka
                ));  
                // await this.kafkaProducer_priceU.ProduceAsync(partitionID.ToString(), serializedObject);

                productToUpdate.price = newPrice;
                
                this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, DateTime.UtcNow));
                Console.WriteLine("[Send]  [Tid: {0}]  [Seller: {1}]  [PriceUpdate]", tid, this.sellerId); 
            }

            finally
            {
                Monitor.Exit(locked);
            }                                
        }

        // todo : ?need change?
        public void UpdateProduct(int tid)
        {
            int idx = this.productIdGenerator.Sample() - 1;
            Console.WriteLine(" -- 1 -- UpdateProduct  [Tid: {0}]  [Seller: {1}]", tid, this.sellerId); 
            object locked = products[idx];

            // only one update of a given version is allowed
            while(!Monitor.TryEnter(locked))
            {
                idx = this.productIdGenerator.Sample() - 1;
                locked = products[idx];
            }

            Console.WriteLine(" -- 2 -- UpdateProduct  [Tid: {0}]  [Seller: {1}, idx {2}]", tid, this.sellerId, idx); 
            
            try
            {
                Product product = new Product(products[idx], tid);
                SendProductUpdateRequest(product, tid);
                // trick so customer do not need to synchronize to get a product (it may refer to an older version though)
                this.products[idx] = product;
            }
            finally
            {
                Monitor.Exit(locked);
            }
        }

        private async void SendProductUpdateRequest(Product product, int tid)
        {
            string obj = JsonConvert.SerializeObject(product);                        
            
            int partitionID = (int)(product.product_id) % config.productPartion;                                    

            Console.WriteLine("[Send]  [Tid: {0}]  [Seller: {1}]  [ProductUpdate]", tid, this.sellerId); 


            // int partitionkafka = randomKafka.Next(10);
            int partitionkafka = 0;   
            await StatefunShared.ProductUpdateMessages.Writer.WriteAsync(
                new KafkaTransactionMessage(
                        partitionID.ToString(),
                        obj,
                        partitionkafka
            )); 
            // await this.kafkaProducer_proU.ProduceAsync(partitionID.ToString(), obj);


            // Console.WriteLine("Seller Thread" + this.sellerId + "ProductUpdate sent to queue");
            // Console.WriteLine(" (++) Tid : " + tid + " Seller " + this.sellerId + " ProductUpdate"); 
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_PRODUCT, DateTime.UtcNow));                                
        }
    }
}