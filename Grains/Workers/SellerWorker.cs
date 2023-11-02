using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Common;
using Common.Http;
using Common.Entities;
using Common.Workload.Seller;
using Common.Streaming;
using Common.Distribution.YCSB;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;
using Common.Distribution;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Requests;
using Common.Response;
using Client.Streaming.Redis;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Threading;
using System.Collections.Concurrent;
using Orleans.Concurrency;
using Client.Streaming.Kafka;

namespace Grains.Workers
{
    [Reentrant]
	public class SellerWorker : Grain, ISellerWorker
    {
        private readonly Random random;

        private SellerWorkerConfig config;

        private IStreamProvider streamProvider;

        private IAsyncStream<Event> kafkaStream;

        private Dictionary<string, KafkaProducer> kafkaProducersForTrans;

        private long sellerId;

        private SellerWorkerStatus status;

        private NumberGenerator productIdGenerator;

        private readonly ILogger<SellerWorker> _logger;

        // to support: (i) customer product retrieval and (ii) the delete operation, since it uses a search string
        private List<Product> products;

        private IAsyncStream<SellerWorkerStatusUpdate> txStream;

        private readonly IDictionary<long,byte> deletedProducts;
        
        private readonly IDictionary<long,TransactionIdentifier> submittedTransactions;
        private readonly IDictionary<long, TransactionOutput> finishedTransactions;

        private bool endToEndLatencyCollection = false;
        private CancellationTokenSource token = new CancellationTokenSource();
        private Task externalTask;
        private string channel;

        readonly String StateFunNamespace = "/e-commerce.fns/";
        readonly String StateFunHttpContentType = "application/vnd.e-commerce.types/";

        public SellerWorker(ILogger<SellerWorker> logger)
        {
            this._logger = logger;
            this.random = new Random();
            this.status = SellerWorkerStatus.IDLE;
            this.deletedProducts = new ConcurrentDictionary<long,byte>();
            this.submittedTransactions = new ConcurrentDictionary<long, TransactionIdentifier>();
            this.finishedTransactions = new ConcurrentDictionary<long, TransactionOutput>();
        }

        private sealed class ProductComparer : IComparer<Product>
        {
            public int Compare(Product x, Product y)
            {
                if (x.product_id < y.product_id) return 0; return 1;
            }
        }

        private static readonly ProductComparer productComparer = new ProductComparer();

        public Task Init(SellerWorkerConfig sellerConfig, List<Product> products, bool endToEndLatencyCollection, string connection)
        {
            // this._logger.LogWarning("Init -> Seller worker {0} with #{1} product(s).", this.sellerId, products.Count);
            Console.WriteLine("[Seller worker] {0} Init", this.sellerId);
            this.config = sellerConfig;        
            this.products = products;
            this.products.Sort(productComparer);
            int firstId = (int)this.products[0].product_id;
            int lastId = (int)this.products.ElementAt(this.products.Count - 1).product_id;

            // this._logger.LogWarning("Init -> Seller worker {0} first {1} last {2}.", this.sellerId, this.products.ElementAt(0).product_id, lastId);

            this.productIdGenerator = this.config.keyDistribution == DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(((lastId - firstId) * 0.3) + firstId), firstId, lastId) :
                this.config.keyDistribution == DistributionType.UNIFORM ?
                 new UniformLongGenerator(firstId, lastId) :
                 new ZipfianGenerator(firstId, lastId);

            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync()
        {
            this.sellerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            this.kafkaStream = streamProvider.GetStream<Event>(StreamingConstants.SellerReactStreamId, this.sellerId.ToString());

            var subscriptionHandles = await kafkaStream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ReactToKafkaResponse);
                }
            }
            // todo
            await this.kafkaStream.SubscribeAsync<Event>(ReactToKafkaResponse);
            
            var workloadStream = streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerStreamId, this.sellerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync(Run);

            this.txStream = streamProvider.GetStream<SellerWorkerStatusUpdate>(StreamingConstants.SellerStreamId, StreamingConstants.TransactionStreamNameSpace);
        }

        private async Task Run(TransactionInput txId, StreamSequenceToken token)
        {
            this.status = SellerWorkerStatus.RUNNING;
            switch (txId.type)
            {
                case TransactionType.DASHBOARD:
                {
                    await BrowseDashboard(txId.tid);
                    break;
                }
                case TransactionType.DELETE_PRODUCT:
                {
                    await DeleteProduct(txId.tid);
                    break;
                }
                case TransactionType.PRICE_UPDATE:
                {
                    await UpdatePrice(txId.tid);
                    break;
                }
            }
            if (config.targetPlatform != Common.TargetPlatform.STATEFUN) {
                this.status = SellerWorkerStatus.IDLE;
                // let emitter aware this request has finished
                _ = txStream.OnNextAsync(new SellerWorkerStatusUpdate(this.sellerId, this.status));
            }
        }

        private async Task BrowseDashboard(int tid)
        {
            if (config.targetPlatform != Common.TargetPlatform.STATEFUN) 
            {
                await Task.Run(() =>
                {
                    try
                    {
                        this._logger.LogWarning("Seller {0}: Dashboard will be queried...", this.sellerId);
                        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.sellerUrl + "/" + this.sellerId);
                        this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.DASHBOARD, DateTime.Now));
                        var response = HttpUtils.client.Send(message);
                        response.EnsureSuccessStatusCode();
                        this.finishedTransactions.Add(tid, new TransactionOutput(tid, DateTime.Now));
                        this._logger.LogWarning("Seller {0}: Dashboard retrieved.", this.sellerId);
                    }
                    catch (Exception e)
                    {
                        this._logger.LogError("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
                    }
                });
            } 
            else
            {
                long partitionID = this.sellerId;
                // string url = config.sellerUrl + StateFunNamespace + "seller" + "/" + partitionID;  

                // await Task.Run(() =>{                        
                JObject payloadObject = new JObject();
                payloadObject["tid"] = tid;
                string payload = JsonConvert.SerializeObject(payloadObject);    

                // string url = config.sellerUrl + StateFunNamespace + "seller" + "/" + partitionID;
                // string contentType = StateFunHttpContentType + "QueryDashboard";      
                Console.WriteLine("[Seller worker {0} | Tid {1}]  HTTP request sent : QueryDashboard", this.sellerId, tid);    
                this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.DASHBOARD, DateTime.Now));            
                // return HttpUtils.SendHttpToStatefun(url, contentType, payload);

                var kafkaProducerWorker = GrainFactory.GetGrain<IKafkaProducerWorker>(0);
                await kafkaProducerWorker.Publish("queryDashboard", partitionID.ToString(), payload);
                    // return this.kafkaProducersForTrans["queryDashboard"].ProduceAsync(, payload);                   
                // });
            }
        }

        private async Task DeleteProduct(int tid)
        {
            if(this.deletedProducts.Count() == this.products.Count())
            {
                this._logger.LogWarning("All products already deleted by seller {0}", this.sellerId);
                return;
            }

            long selectedProduct = this.productIdGenerator.NextValue();

            while (deletedProducts.ContainsKey(selectedProduct))
            {
                selectedProduct = this.productIdGenerator.NextValue();
            }
            if (config.targetPlatform != Common.TargetPlatform.STATEFUN)
            {
                await Task.Run(() =>
                {
                    try
                    {
                        var obj = JsonConvert.SerializeObject(new DeleteProduct(sellerId, selectedProduct, tid));
                        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Delete, config.productUrl);
                        message.Content = HttpUtils.BuildPayload(obj);
                        this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.DELETE_PRODUCT, DateTime.Now));
                        // this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.DELETE_PRODUCT, DateTime.Now));
                        var response = HttpUtils.client.Send(message);
                        response.EnsureSuccessStatusCode();
                        // many threads can access the set at the same time...
                        this.deletedProducts.Add(selectedProduct, 0);
                        this._logger.LogWarning("Seller {0}: Product {1} deleted.", this.sellerId, selectedProduct);
                    }
                    catch (Exception e) {
                        this._logger.LogError("Seller {0}: Product {1} could not be deleted: {2}", this.sellerId, selectedProduct, e.Message);
                    }
                });
            }
            else
            {                       
                var payload = JsonConvert.SerializeObject(new DeleteProduct(sellerId, selectedProduct, tid));                     
                int partitionId = (int)(selectedProduct % config.productPartion);
                // string url = config.productUrl + StateFunNamespace + "product" + "/" + partitionId;
                // string contentType = StateFunHttpContentType + "DeleteProduct";
                Console.WriteLine("[Seller worker {0} | Tid {1}]  HTTP request sent : DeleteProduct-{2}", this.sellerId, tid, selectedProduct);    
                // this._logger.LogWarning("DeleteProduct productID: {0} start", selectedProduct);
                // this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.DELETE_PRODUCT, DateTime.Now));
                this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.DELETE_PRODUCT, DateTime.Now));
                // return HttpUtils.SendHttpToStatefun(url, contentType, obj);                     
                // return this.kafkaProducersForTrans["productDelete"].ProduceAsync(partitionId.ToString(), obj);                                                 
                var kafkaProducerWorker = GrainFactory.GetGrain<IKafkaProducerWorker>(0);
                await kafkaProducerWorker.Publish("productDelete", partitionId.ToString(), payload);
            }
        }

        private async Task<List<Product>> GetOwnProducts()
        {
            HttpResponseMessage response = await Task.Run(async () =>
            {
                // [query string](https://en.wikipedia.org/wiki/Query_string)
                return await HttpUtils.client.GetAsync(config.productUrl + "/" + this.sellerId);
            });
            // deserialize response
            response.EnsureSuccessStatusCode();
            string productsStr = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<List<Product>>(productsStr);
        }

        private async Task UpdatePrice(int tid)
        {
            // to simulate how a seller would interact with the platform
            // this._logger.LogWarning("Seller {0} has started UpdatePrice", this.sellerId);
            // Console.WriteLine("[Seller worker] {0} UpdatePrice start", this.sellerId);

            // 1 - simulate seller browsing own main page (that will bring the product list)
            if (config.targetPlatform != Common.TargetPlatform.STATEFUN) {        
                var productsRetrieved = (await GetOwnProducts()).ToDictionary(k=>k.product_id,v=>v);
                int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                await Task.Delay(delay);
            }

            // 2 - select one to submit the update (based on distribution)
            long selectedProduct = this.productIdGenerator.NextValue();

            while (this.deletedProducts.ContainsKey(selectedProduct))
            {
                selectedProduct = this.productIdGenerator.NextValue();
            }

            // define perc to raise
            int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);

            // 3 - get new price            
            var currPrice = this.products.Where(p=>p.product_id == selectedProduct).FirstOrDefault().price;            
            var newPrice = currPrice + ( (currPrice * percToAdjust) / 100 );
            
            // 4 - submit update
            string payload = JsonConvert.SerializeObject(new UpdatePrice(this.sellerId, selectedProduct, newPrice, tid));
            if (config.targetPlatform == Common.TargetPlatform.STATEFUN)
            {                
                int partitionID = (int)(selectedProduct) % config.productPartion;
                // string url = config.productUrl + StateFunNamespace + "product" + "/" + partitionID;
                // string contentType = StateFunHttpContentType + "UpdateSinglePrice";      
                // this._logger.LogWarning("UpdatePrice productID: {0} start", selectedProduct);     
                Console.WriteLine("[Seller worker {0} | Tid {1}]  HTTP request sent : UpdatePrice-{2}", this.sellerId, tid, selectedProduct);    
                this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, DateTime.Now));            
                // return HttpUtils.SendHttpToStatefun(url, contentType, serializedObject);
                // return this.kafkaProducersForTrans["priceUpdate"].ProduceAsync(partitionID.ToString(), serializedObject);                                   
                var kafkaProducerWorker = GrainFactory.GetGrain<IKafkaProducerWorker>(0);
                await kafkaProducerWorker.Publish("priceUpdate", partitionID.ToString(), payload);
            }
            else 
            {            
                var resp = await Task.Run(() =>
                {
                    // https://dev.olist.com/docs/editing-a-product
                    HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Patch, config.productUrl);
                    request.Content = HttpUtils.BuildPayload(payload);
                    this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, DateTime.Now));
                    return HttpUtils.client.Send(request);
                });
                if (resp.IsSuccessStatusCode)
                {
                    this._logger.LogWarning("Seller {0}: Finished product {1} price update.", this.sellerId, selectedProduct);
                    return;
                }
                this._logger.LogError("Seller {0} failed to update product {1} price.", this.sellerId, selectedProduct);
            }            
        }

        private sealed class ProductEqualityComparer : IEqualityComparer<Product>
        {
            public bool Equals(Product x, Product y)
            {
                return (x.product_id == y.product_id);
            }

            public int GetHashCode([DisallowNull] Product obj)
            {
                return obj.product_id.GetHashCode();
            }
        }

        public Task<long> GetProductId()
        {
            long selectedProduct = this.productIdGenerator.NextValue();
            return Task.FromResult(selectedProduct);
        }

        public Task<Product> GetProduct()
        {
            long selectedProduct = this.productIdGenerator.NextValue();
            Product product = this.products.Where(p => p.product_id == selectedProduct).FirstOrDefault();
            return Task.FromResult(product);
        }

        public Task<List<Latency>> Collect(DateTime startTime)
        {
            var targetValues = submittedTransactions.Values.Where(e => e.startTs.CompareTo(startTime) >= 0);
            var latencyList = new List<Latency>(submittedTransactions.Count());
            foreach (var entry in targetValues)
            {
                if (finishedTransactions.ContainsKey(entry.tid))
                {
                    var res = finishedTransactions[entry.tid];
                    latencyList.Add(new Latency(entry.tid, entry.type,
                        (res.timestamp - entry.startTs).TotalMilliseconds ));
                }
            }
            return Task.FromResult(latencyList);
        }

        private Task ReactToKafkaResponse(Event responseEvent, StreamSequenceToken token)
        {            
            kafkaResponse response = JsonConvert.DeserializeObject<kafkaResponse>(responseEvent.payload);
            int tid = response.tid;        
            this.finishedTransactions.Add(tid, new TransactionOutput(tid, DateTime.Now));

            // this._logger.LogWarning("(+++ Kafka +++) task:{0} -- Tid:{1} -- taskId:{2} -- success:{3}",responseEvent.topic, tid, response.taskId, response.result);
            
            if (responseEvent.topic == "deleteProductTask") {                     
                this.deletedProducts.Add(response.taskId, 0);                               
                Console.WriteLine(" ^-^ [Seller worker {0} | Tid {1}] : Kafka received deleteProduct ", this.sellerId, response.tid);
                // this._logger.LogWarning("Seller {0}: Product {1} Delete", this.sellerId, response.taskId); 
            } else if (responseEvent.topic == "updatePriceTask") {                
                Console.WriteLine(" ^-^ [Seller worker {0} | Tid {1}] : Kafka received updatePriceTask ", this.sellerId, response.tid);
                // this._logger.LogWarning("Seller {0}: Product {1} UpdatePrice", this.sellerId, response.taskId); 
            } else if (responseEvent.topic == "queryDashboardTask") { 
                Console.WriteLine(" ^-^ [Seller worker {0} | Tid {1}] : Kafka received queryDashboardTask ", this.sellerId, response.tid);
                // this._logger.LogWarning("Seller {0}: Product {1} QueryDashboard", this.sellerId, response.taskId);
            }
            else {
                throw new Exception("Unknown topic: " + responseEvent.topic);
            }            

            this.status = SellerWorkerStatus.IDLE;
            // let emitter aware this request has finished
            _ = txStream.OnNextAsync(new SellerWorkerStatusUpdate(this.sellerId, this.status));                            
            return Task.CompletedTask;
        }
    }
}