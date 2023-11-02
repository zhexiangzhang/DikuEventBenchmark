using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Common.Http;
using Common.Workload.Customer;
using Common.Entities;
using Common.Streaming;
using Common.Distribution.YCSB;
using Common.Response;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;
using Common.Distribution;
using Common.Requests;
using Common.Workload;
using Common.Workload.Metrics;
using Client.Streaming.Redis;
using System.Threading;
using Newtonsoft.Json.Linq;
using Client.Streaming.Kafka;

namespace Grains.Workers
{

    /**
     * Driver-side, client-side code, which is also run in Orleans silo
     * Nice example of tasks in a customer session:
     * https://github.com/GoogleCloudPlatform/microservices-demo/blob/main/src/loadgenerator/locustfile.py
     */
    public sealed class CustomerWorker : Grain, ICustomerWorker
    {
        private readonly Random random;

        private CustomerWorkerConfig config;

        private NumberGenerator sellerIdGenerator;

        private IStreamProvider streamProvider;

        private IAsyncStream<Event> kafkaStream;

        private Dictionary<string, KafkaProducer> kafkaProducersForTrans;

        private IAsyncStream<CustomerWorkerStatusUpdate> txStream;

        private long customerId;

        private Customer customer;

        private CustomerWorkerStatus status;

        private readonly IDictionary<long, TransactionIdentifier> submittedTransactions;
        private readonly IDictionary<long, TransactionOutput> finishedTransactions;

        private readonly ILogger<CustomerWorker> logger;

        private CancellationTokenSource token = new CancellationTokenSource();
        private Task externalTask;
        private string channel;

        // first long is sellerId, second long is productId
        // private Dictionary<(long, long), bool> addCartResult;

        private int tid;

        readonly String StateFunNamespace = "/e-commerce.fns/";
        readonly String StateFunHttpContentType = "application/vnd.e-commerce.types/";

        public override async Task OnActivateAsync()
        {
            this.customerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            this.kafkaStream = streamProvider.GetStream<Event>(StreamingConstants.CustomerReactStreamId, this.customerId.ToString());
            
            var subscriptionHandles = await kafkaStream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ReactToKafkaResponse);
                }
            }
            await kafkaStream.SubscribeAsync<Event>(ReactToKafkaResponse);

            var workloadStream = streamProvider.GetStream<int>(StreamingConstants.CustomerStreamId, this.customerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync(Run);
            
            // Console.WriteLine("Customer {0} activated statefun", customerId);

            // to notify transaction orchestrator about status update
            this.txStream = streamProvider.GetStream<CustomerWorkerStatusUpdate>(StreamingConstants.CustomerStreamId, StreamingConstants.TransactionStreamNameSpace);
        }

        public CustomerWorker(ILogger<CustomerWorker> logger)
        {
            this.logger = logger;
            this.random = new Random();
            this.status = CustomerWorkerStatus.IDLE;
            this.submittedTransactions = new Dictionary<long, TransactionIdentifier>();
            this.finishedTransactions = new Dictionary<long, TransactionOutput>();
            // this.addCartResult = new Dictionary<(long, long), bool>();
        }

        public Task Init(CustomerWorkerConfig config, Customer customer, bool endToEndLatencyCollection, string connection = "")
        {
            // this.logger.LogWarning("Customer worker {0} Init", this.customerId);
            Console.WriteLine("[Customer worker] {0} Init", this.customerId);
            this.config = config;
            this.customer = customer;            
            this.sellerIdGenerator = this.config.sellerDistribution == DistributionType.NON_UNIFORM ?
                new NonUniformDistribution( (int)(this.config.sellerRange.max * 0.3), this.config.sellerRange.min, this.config.sellerRange.max) :
                this.config.sellerDistribution == DistributionType.UNIFORM ?
                new UniformLongGenerator(this.config.sellerRange.min, this.config.sellerRange.max) :
                new ZipfianGenerator(this.config.sellerRange.min, this.config.sellerRange.max);

            return Task.CompletedTask;
        }

        /**
         * From the list of browsed keys, picks randomly the keys to checkout
         */
        private ISet<(long sellerId, long productId)> DefineKeysToCheckout(List<(long sellerId, long productId)> browsedKeys, int numberOfKeysToCheckout)
        {
            ISet<(long sellerId, long productId)> set = new HashSet<(long sellerId, long productId)>(numberOfKeysToCheckout);
            while (set.Count < numberOfKeysToCheckout)
            {
                set.Add(browsedKeys[random.Next(0, browsedKeys.Count)]);
            }
            return set;
        }

        /**
         * StateFun:
         *    For statefun because we cant follow request-response pattern
         */
        private async Task<ISet<Product>> DefineProductsToCheckout(int numberOfKeysToBrowse){        
            ISet<Product> products = new HashSet<Product>(numberOfKeysToBrowse);
            ISet<(long sellerId, long productId)> keyMap = new HashSet<(long sellerId, long productId)>(numberOfKeysToBrowse);

            ISellerWorker sellerWorker;
            StringBuilder sb = new StringBuilder();
            long sellerId;        
            Product product;

            for (int i = 0; i < numberOfKeysToBrowse; i++)
            {
                sellerId = this.sellerIdGenerator.NextValue();
                sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);

                // we dont measure the performance of the benchmark, only the system. as long as we can submit enough workload we are fine
            
                product = await sellerWorker.GetProduct();
                long productId = product.product_id;

                while (keyMap.Contains((sellerId,productId)))
                {
                    sellerId = this.sellerIdGenerator.NextValue();
                    sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);
                    product = await sellerWorker.GetProduct();
                    productId = product.product_id;
                }
                
                products.Add(product);
                keyMap.Add((sellerId,productId));
                sb.Append(sellerId).Append("-").Append(productId);                            
                if (i < numberOfKeysToBrowse - 1) sb.Append(" | ");
            }
            // this.logger.LogWarning("Customer {0} defined the keys to browse: {1}", this.customerId, sb.ToString());
            Console.WriteLine(" --- [Customer worker {0} | Tid {1}]  defined the keys to browse: {2}", this.customerId, tid, sb.ToString());
            return products;
        }

        private async Task<ISet<(long sellerId,long productId)>> DefineKeysToBrowseAsync(int numberOfKeysToBrowse)
        {
            ISet<(long sellerId, long productId)> keyMap = new HashSet<(long sellerId, long productId)>(numberOfKeysToBrowse);
            ISellerWorker sellerWorker;
            StringBuilder sb = new StringBuilder();
            long sellerId;
            long productId;
            for (int i = 0; i < numberOfKeysToBrowse; i++)
            {
                sellerId = this.sellerIdGenerator.NextValue();
                sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);

                // we dont measure the performance of the benchmark, only the system. as long as we can submit enough workload we are fine
                productId = await sellerWorker.GetProductId();
                while (keyMap.Contains((sellerId,productId)))
                {
                    sellerId = this.sellerIdGenerator.NextValue();
                    sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);
                    productId = await sellerWorker.GetProductId();
                }

                keyMap.Add((sellerId,productId));
                sb.Append(sellerId).Append("-").Append(productId);
                if (i < numberOfKeysToBrowse - 1) sb.Append(" | ");
            }
            // this.logger.LogWarning("Customer {0} defined the keys to browse: {1}", this.customerId, sb.ToString());
            return keyMap;
        }

        private async Task Run(int tid, StreamSequenceToken token)
        {
            // this.logger.LogWarning("Customer worker {0} processing new task...", this.customerId);
            Console.WriteLine("[Customer worker {0} | Tid {1}] processing new task...", this.customerId, tid);

            this.status = CustomerWorkerStatus.BROWSING;

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

            // this.logger.LogWarning("Customer {0} number of keys to browse: {1}", customerId, numberOfKeysToBrowse);            

            this.tid = tid;

            if (config.targetPlatform != Common.TargetPlatform.STATEFUN)
            {
                var keyMap = await DefineKeysToBrowseAsync(numberOfKeysToBrowse);            

                this.logger.LogWarning("Customer {0} started browsing...", this.customerId);

                // browsing
                await Browse(keyMap);

                this.logger.LogWarning("Customer worker {0} finished browsing.", this.customerId);

                // TODO should we also model this behavior?
                int numberOfKeysToCheckout =
                    random.Next(1, Math.Min(numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart) + 1);

                // adding to cart
                try
                {                
                    await AddToCart(DefineKeysToCheckout(keyMap.ToList(), numberOfKeysToCheckout));
                }
                catch (Exception e)
                {
                    this.logger.LogError(e.Message);
                    this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                    await InformFailedCheckout(tid);
                    return; // no need to continue  
                }

                await GetCart();

                await Checkout(tid);
            } 
            else 
            {
                int numberOfKeysToCheckout =
                    random.Next(1, Math.Min(numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart) + 1);
                
                // adding to cart and checkout
                try
                {  
                    // Console.WriteLine("Customer {0} in statefun addtocart", customerId);
                    ISet<Product> products = await DefineProductsToCheckout(numberOfKeysToCheckout);
                    await AddToCart(products);
                    await Checkout(tid);
                }
                catch (Exception e)
                {
                    this.logger.LogError(e.Message);
                    this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                    await InformFailedCheckout(tid);
                    return; // no need to continue  
                }
                // Console.WriteLine("Customer {0} in statefun checkout", customerId);
                // await Checkout(tid);
            }
        }

        private async Task UpdateStatusAsync(int tid)
        {
            this.status = CustomerWorkerStatus.IDLE;
            await txStream.OnNextAsync(new CustomerWorkerStatusUpdate(this.customerId, this.status)); 
        }

        private async Task InformFailedCheckout(int tid)
        {
            if (config.targetPlatform != Common.TargetPlatform.STATEFUN) 
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/seal");
                HttpUtils.client.Send(message);
            } 
            else
            {
                // this.addCartResult.Clear();
                CustomerSession session = new CustomerSession
                {
                    Type = "clearCart",
                    CartItem = null,
                    CustomerCheckout = null
                };

                string payload = JsonConvert.SerializeObject(session);
                
                int partitionID = (int)(this.customerId);                    
                Console.WriteLine("[Customer worker {0} | Tid {1}] : HTTP request sent : clear cart as checkout Fail", this.customerId, tid);                        
                var kafkaProducerWorker = GrainFactory.GetGrain<IKafkaProducerWorker>(0);
                await kafkaProducerWorker.Publish("customerSession", partitionID.ToString(), payload);
            }
            // just cleaning cart state for next browsing
            Console.WriteLine(" [Customer worker {0} | Tid {1}] : checkout Fail", this.customerId, tid);
            await txStream.OnNextAsync(new CustomerWorkerStatusUpdate(this.customerId, this.status));        
        }

        private async Task InformSucceededCheckout(int tid)
        {
            this.status = CustomerWorkerStatus.CHECKOUT_SENT;
            // this.logger.LogWarning("Customer {0} sent the checkout successfully", customerId);
            // Console.WriteLine(" ++kafka [Customer worker] {0} checkout successfully", this.customerId);
            Console.WriteLine(" ^-^ [Customer worker {0} | Tid {1}] : checkout successfully", this.customerId, tid);
            await UpdateStatusAsync(tid);
        }

        /**
         * Simulating the customer browsing the cart before checkout
         * could verify whether the cart contains all the products chosen, otherwise throw exception
         */
        private async Task GetCart()
        {
            await Task.Run(() =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, this.config.cartUrl + "/" + this.customerId);
                return HttpUtils.client.Send(message);
            });
        }

        private async Task Checkout(int tid)
        {
            // define whether client should send a checkout request
            if (random.Next(0, 100) > this.config.checkoutProbability)
            {
                this.status = CustomerWorkerStatus.CHECKOUT_NOT_SENT;
                // this.logger.LogWarning("Customer {0} decided to not send a checkout.", this.customerId);
                Console.WriteLine("[Customer worker {0} | Tid {1}] : decided to not send a checkout.", this.customerId, tid);
                await InformFailedCheckout(tid);
                return;
            }

            // this.logger.LogWarning("Customer {0} decided to send a checkout.", this.customerId);

            if (config.targetPlatform !=  Common.TargetPlatform.STATEFUN) {

                // inform checkout intent. optional feature
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, this.config.cartUrl + "/" + this.customerId + "/checkout");
                
                message.Content = HttpUtils.BuildPayload(BuildCheckoutPayload(tid, this.customer));

                TransactionIdentifier txId = null;
                HttpResponseMessage resp = await Task.Run(() =>
                {
                    txId = new TransactionIdentifier(tid, TransactionType.CUSTOMER_SESSION, DateTime.Now);
                    return HttpUtils.client.Send(message);
                });

                if (resp == null)
                {
                    this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                    await InformFailedCheckout(tid);
                    return;
                }
                if (resp.IsSuccessStatusCode)
                {
                    submittedTransactions.Add(txId.tid, txId);
                    await InformSucceededCheckout(tid);
                    return;
                }

                // perhaps there is price divergence. checking out again means the customer agrees with the new prices
                if(resp.StatusCode == HttpStatusCode.MethodNotAllowed)
                {
                    resp = await Task.Run(() =>
                    {
                        txId = new TransactionIdentifier(tid, TransactionType.CUSTOMER_SESSION, DateTime.Now);
                        return HttpUtils.client.Send(message);
                    });
                }

                if (resp == null || !resp.IsSuccessStatusCode)
                {
                    this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                    await InformFailedCheckout(tid);
                    return;
                }

                submittedTransactions.Add(txId.tid, txId);
                // very unlikely to have another price update (considering the distribution is low)
                await InformSucceededCheckout(tid);
            }
            else 
            {                                                    
                int partitionID = (int)(this.customerId);   
                // checkout send to cart (1:1)             
                // string url = config.cartUrl + StateFunNamespace + "cart" + "/" + partitionID;
                // string contentType = StateFunHttpContentType + "CheckoutCart";    
                string payload = BuildCheckoutPayload(tid, this.customer);

                // JObject newObject = new JObject();
                // newObject["customerCheckout"] = JObject.Parse(payload);
                // payload = JsonConvert.SerializeObject(newObject);
                
                TransactionIdentifier txId = null;                     

                // await Task.Run(() =>
                // {
                txId = new TransactionIdentifier(tid, TransactionType.CUSTOMER_SESSION, DateTime.Now);
                submittedTransactions.Add(txId.tid, txId);
                Console.WriteLine("[Customer worker {0} | Tid {1}] : HTTP request sent : checkout", this.customerId, tid);
                // return HttpUtils.SendHttpToStatefun(url, contentType, payload);
                // return this.kafkaProducersForTrans["checkout"].ProduceAsync(partitionID.ToString(), payload);                         
                var kafkaProducerWorker = GrainFactory.GetGrain<IKafkaProducerWorker>(0);
                await kafkaProducerWorker.Publish("customerSession", partitionID.ToString(), payload);
                // });

            }
        }

        /**
         * StateFun
         */
        private async Task AddToCart(ISet<Product> products)
        {
            foreach (var product in products)
            {                
                try
                {
                    var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);

                    string productRet = JsonConvert.SerializeObject(product);

                    JObject newObject = new JObject();

                    string payload = BuildCartItem(productRet, qty);

                    int partitionID = (int)(this.customerId);
                    long sellerID = product.seller_id;                
                                        
                    Console.WriteLine("[Customer worker {0} | Tid {1}] : HTTP request sent : addItemToCart-{2}... ", this.customerId, tid, product.product_id);
                  
                    var kafkaProducerWorker = GrainFactory.GetGrain<IKafkaProducerWorker>(0);
                    await kafkaProducerWorker.Publish("customerSession", partitionID.ToString(), payload);
                }
                catch (Exception e)
                {
                    this.logger.LogWarning("Customer {0} Url {1} Seller {2} Key {3}: Exception Message: {5} ",  customerId, this.config.productUrl, product.seller_id, product.product_id, e.Message);
                }                

                int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                // artificial delay after adding the product
                await Task.Delay(delay);
            }
        }

        private async Task AddToCart(ISet<(long sellerId, long productId)> keyMap)
        {            
            foreach (var entry in keyMap)
            {
                this.logger.LogWarning("Customer {0}: Adding seller {1} product {2} to cart", this.customerId, entry.sellerId, entry.productId);
                await Task.Run(() =>
                {
                    HttpResponseMessage response;
                    try
                    {
                        HttpRequestMessage message1 = new HttpRequestMessage(HttpMethod.Get, this.config.productUrl + "/" + entry.sellerId + "/" + entry.productId);
                        response = HttpUtils.client.Send(message1);

                        // add to cart
                        if (response.Content.Headers.ContentLength == 0)
                        {
                            this.logger.LogWarning("Customer {0}: Response content for seller {1} product {2} is empty!", this.customerId, entry.sellerId, entry.productId);
                            return;
                        }

                        this.logger.LogWarning("Customer {0}: seller {1} product {2} retrieved", this.customerId, entry.sellerId, entry.productId);

                        var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);

                        var stream = response.Content.ReadAsStream();

                        StreamReader reader = new StreamReader(stream);
                        string productRet = reader.ReadToEnd();
                        var payload = BuildCartItem(productRet, qty);

                        HttpRequestMessage message2 = new HttpRequestMessage(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/add");
                        message2.Content = HttpUtils.BuildPayload(payload);

                        this.logger.LogWarning("Customer {0}: Sending seller {1} product {2} payload to cart...", this.customerId, entry.sellerId, entry.productId);
                        response = HttpUtils.client.Send(message2);
                    }
                    catch (Exception e)
                    {
                        this.logger.LogWarning("Customer {0} Url {1} Seller {2} Key {3}: Exception Message: {5} ",  customerId, this.config.productUrl, entry.sellerId, entry.productId, e.Message);
                    }

                });

                int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                // artificial delay after adding the product
                await Task.Delay(delay);

            }

        }

        private async Task Browse(ISet<(long sellerId, long productId)> keyMap)
        {
            int delay;
            foreach (var entry in keyMap)
            {
                this.logger.LogWarning("Customer {0} browsing seller {1} product {2}", this.customerId, entry.sellerId, entry.productId);
                delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                await Task.Run(async () =>
                {
                    try
                    {
                        await HttpUtils.client.GetAsync(this.config.productUrl + "/" + entry.sellerId + "/" + entry.productId); 
                    }
                    catch (Exception e)
                    {
                        this.logger.LogWarning("Exception Message: {0} Customer {1} Url {2} Seller {3} Product {4}", e.Message, customerId, this.config.productUrl, entry.sellerId, entry.productId);
                    }

                });
                // artificial delay after retrieving the product
                await Task.Delay(delay);
            }
        }

        private String BuildCartItem(string productPayload, int quantity)
        {

            Product product = JsonConvert.DeserializeObject<Product>(productPayload);

            // define voucher from distribution
            var vouchers = Array.Empty<decimal>();
            int probVoucher = this.random.Next(0, 101);
            if(probVoucher <= this.config.voucherProbability)
            {
                int numVouchers = this.random.Next(1, this.config.maxNumberVouchers + 1);
                vouchers = new decimal[numVouchers];
                for(int i = 0; i < numVouchers; i++)
                {
                    vouchers[i] = this.random.Next(1, 10);
                }
            }
            
            // build a basket item
            CartItem basketItem = new CartItem(
                    product.seller_id,
                    product.product_id,
                    product.name,
                    product.price,
                    product.freight_value,
                    quantity,
                    vouchers
            );

            CustomerSession session = new CustomerSession
            {
                Type = "addToCart",
                CartItem = basketItem,
                CustomerCheckout = null
            };
            
            // string payload = JsonConvert.SerializeObject(basketItem);
            string payload = JsonConvert.SerializeObject(session);
            // return HttpUtils.BuildPayload(payload);    
            return payload;
        }

        private String BuildCheckoutPayload(int tid, Customer customer)
        {

            // define payment type randomly
            var typeIdx = random.Next(1, 4);
            PaymentType type = typeIdx > 2 ? PaymentType.CREDIT_CARD : typeIdx > 1 ? PaymentType.DEBIT_CARD : PaymentType.BOLETO;

            // build
            CustomerCheckout basketCheckout = new CustomerCheckout(
                customer.id,
                customer.first_name,
                customer.last_name,
                customer.city,
                customer.address,
                customer.complement,
                customer.state,
                customer.zip_code,
                type.ToString(),
                customer.card_number,
                customer.card_holder_name,
                customer.card_expiration,
                customer.card_security_number,
                customer.card_type,
                random.Next(1, 11), // installments
                tid
            );
            
            CustomerSession session = new CustomerSession
            {
                Type = "checkout",
                CartItem = null,
                CustomerCheckout = basketCheckout
            };
            
            // string payload = JsonConvert.SerializeObject(basketItem);
            string payload = JsonConvert.SerializeObject(session);

            // var payload = JsonConvert.SerializeObject(basketCheckout);
            // return HttpUtils.BuildPayload(payload);
            return payload;
        }

        private async Task ReactToKafkaResponse(Event responseEvent, StreamSequenceToken token)
        {
            kafkaResponse response = JsonConvert.DeserializeObject<kafkaResponse>(responseEvent.payload);            

            if (responseEvent.topic == "checkoutTask") 
            {
                Console.WriteLine(" ^-^ [Customer worker {0} | Tid {1}] : Kafka received checkout ", this.customerId, response.tid);
                // Console.WriteLine(" ^-^ [Customer worker {0} | Tid {1}] : decide to checkout, (received all add) ", this.customerId, response.tid);
                this.finishedTransactions.Add(tid, new TransactionOutput(tid, DateTime.Now));
                // this.addCartResult.Clear();
                if (response.result == "fail") 
                {
                    this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                    await InformFailedCheckout(tid);
                } 
                else
                {
                    this.status = CustomerWorkerStatus.CHECKOUT_SENT;
                    await InformSucceededCheckout(tid);
                }
            } 
            else 
            {
                throw new Exception("Unknown topic: " + responseEvent.topic);
            }
        
        }

        public Task<List<Latency>> Collect(DateTime startTime)
        {
            // unsubscribe redis
            // this.token.Cancel();

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

    }
}