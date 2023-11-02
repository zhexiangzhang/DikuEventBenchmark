using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Infra;
using Common.Distribution;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Workers;
using Common.Services;
using Common.Entities;
using Newtonsoft.Json;
using Statefun.Entity;
using MathNet.Numerics.Distributions;
using Common.Workload.CustomerWorker;
using Statefun.Workload;
using Statefun.Streaming;
using Common.Requests;

namespace Statefun.Workers
{
    public class StatefunCustomerThread : ICustomerWorker
    {
        private readonly Random random;
        private readonly Random randomKafka;
        private int kafkaPartition;

        private CustomerWorkerConfig config;

        private IDiscreteDistribution sellerIdGenerator;
        private readonly int numberOfProducts;
        private IDiscreteDistribution productIdGenerator;

        // the object respective to this worker
        private Customer customer;

        private readonly List<TransactionIdentifier> submittedTransactions;

        // todo !! 
        private readonly List<TransactionOutput> finishedTransactions;

        private readonly HttpClient httpClient;

        private readonly ISellerService sellerService;

        private readonly ILogger logger;        

        // private KafkaProducer kafkaProducer;        

        public static StatefunCustomerThread BuildCustomerThread(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer)
        {
            var logger = LoggerProxy.GetInstance("Customer" + customer.id.ToString());
            return new StatefunCustomerThread(sellerService, numberOfProducts, config, customer, logger);
        }

        private StatefunCustomerThread(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, ILogger logger)
        {
            this.sellerService = sellerService;            
            this.config = config;
            this.customer = customer;
            this.numberOfProducts = numberOfProducts;
            this.logger = logger;
            this.submittedTransactions = new List<TransactionIdentifier>();
            this.finishedTransactions = new List<TransactionOutput>();
            this.randomKafka = new Random();
            this.random = new Random();
            // Console.WriteLine("%before%  " + customer.id);     
            // this.kafkaProducer = new KafkaProducer("kafkahost:9092", "customerSession");
            // Console.WriteLine("%%  " + customer.id);     
        }

        public void SetDistribution(DistributionType sellerDistribution, Interval sellerRange, DistributionType keyDistribution)
        {
            this.sellerIdGenerator = sellerDistribution == DistributionType.UNIFORM ?
                                  new DiscreteUniform(sellerRange.min, sellerRange.max, new Random()) :
                                  new Zipf(0.80, sellerRange.max, new Random());
            this.productIdGenerator = keyDistribution == DistributionType.UNIFORM ?
                                new DiscreteUniform(1, numberOfProducts, new Random()) :
                                new Zipf(0.99, numberOfProducts, new Random());
        }

        public void Run(int tid)
        {
            this.kafkaPartition = randomKafka.Next(20); 
            AddItemsToCart();            
            Checkout(tid);      
        }

        public void AddItemsToCart()
        {             
            int numberOfProducts = random.Next(1, this.config.maxNumberKeysToAddToCart + 1);
            ISet<(int, int)> set = new HashSet<(int, int)>();               
            while (set.Count < numberOfProducts)
            {                
                var sellerId = this.sellerIdGenerator.Sample();
                int productId = this.productIdGenerator.Sample() - 1;                
                var product = sellerService.GetProduct(sellerId, productId);
                if (set.Add((sellerId, product.product_id)))
                {                    
                    var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);
                    CartItem basketItem = BuildCartItem(product, qty);
                                                            
                    sendCustomerSessionMessageToQueue("addToCart", basketItem, null);
                }
            }            
        }

        public void Checkout(int tid)
        {
            // Console.WriteLine(" ########## [Tid: {0}] [Customer in checkout fun]", tid); 
            // define whether client should send a checkout request
            if (random.Next(0, 100) > this.config.checkoutProbability)
            {
                InformFailedCheckout();
                return;
            }            

            CustomerCheckout basketCheckout = BuildCheckoutPayload(tid, this.customer);
            sendCustomerSessionMessageToQueue("checkout", null, basketCheckout);
            // Console.WriteLine(" (++) Tid : " + tid + " Customer " + customer.id + " checkout"); 
            Console.WriteLine("[Send]  [Tid: {0}]  [Customer: {1}]  [Checkout]", tid, customer.id); 

            var now = DateTime.UtcNow;
            TransactionIdentifier txId = new(tid, TransactionType.CUSTOMER_SESSION, now);
            this.submittedTransactions.Add(txId);            
        }

        private void InformFailedCheckout()
        {
            // just cleaning cart state for next browsing            
            sendCustomerSessionMessageToQueue("clearCart", null, null);         
        }

        public List<TransactionIdentifier> GetSubmittedTransactions()
        {
            return this.submittedTransactions;
        }

        public List<TransactionOutput> GetFinishedTransactions()
        {
            return this.finishedTransactions;
        }

        public void AddFinishedTransaction(TransactionOutput transactionOutput)
        {
            this.finishedTransactions.Add(transactionOutput);
        }

        private async void sendCustomerSessionMessageToQueue(String type, CartItem? cartItem, CustomerCheckout? customerCheckout)
        {
            CustomerSession session = new CustomerSession
            {
                Type = type,
                CartItem = cartItem,
                CustomerCheckout = customerCheckout
            };

            string payload = JsonConvert.SerializeObject(session);
                
            int partitionID = (int)(this.customer.id);

            await StatefunShared.CustomerSessionMessages.Writer.WriteAsync(
                        new KafkaTransactionMessage(
                             partitionID.ToString(),
                             payload,
                             this.kafkaPartition
                    )); 

            // await this.kafkaProducer.ProduceAsync(partitionID.ToString(), payload);

        }

        private CartItem BuildCartItem(Product product, int quantity)
        {
            // define voucher from distribution
            float voucher = 0;
            int probVoucher = this.random.Next(0, 101);
            if (probVoucher <= this.config.voucherProbability)
            {
                voucher = product.price * 0.10f;
            }        

            // build a basket item
            CartItem basketItem = new CartItem(
                    product.seller_id,
                    product.product_id,
                    product.name,
                    product.price,
                    product.freight_value,
                    quantity,
                    voucher,
                    product.version
            );            
            return basketItem;
        }

        private CustomerCheckout BuildCheckoutPayload(int tid, Customer customer)
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
                            
            return basketCheckout;
        }
    }
}