using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Distribution;
using Common.Distribution.YCSB;
using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Customer;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Streams;

namespace Client.Workload
{

    public class WorkloadEmitter : Stoppable
	{

        private readonly IClusterClient orleansClient;

        private readonly IStreamProvider streamProvider;

        public readonly Dictionary<TransactionType, NumberGenerator> keyGeneratorPerWorkloadType;

        private StreamSubscriptionHandle<CustomerWorkerStatusUpdate> customerWorkerSubscription;

        private StreamSubscriptionHandle<SellerWorkerStatusUpdate> sellerWorkerSubscription;

        private StreamSubscriptionHandle<int> deliveryWorkerSubscription;

        private readonly ConcurrentDictionary<long, CustomerWorkerStatus> customerStatusCache;

        private readonly int concurrencyLevel;

        private readonly ILogger logger;

        public WorkloadEmitter(IClusterClient clusterClient,
                                DistributionType sellerDistribution,
                                Interval sellerRange,
                                DistributionType customerDistribution,
                                Interval customerRange,
                                int concurrencyLevel) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            this.concurrencyLevel = concurrencyLevel;

            NumberGenerator sellerIdGenerator = sellerDistribution ==
                                DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(sellerRange.max * 0.3), sellerRange.min, sellerRange.max) :
                                sellerDistribution == DistributionType.UNIFORM ?
                                new UniformLongGenerator(sellerRange.min, sellerRange.max) :
                                new ZipfianGenerator(sellerRange.min, sellerRange.max);

            NumberGenerator customerIdGenerator = customerDistribution ==
                                DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(customerRange.max * 0.3), customerRange.min, customerRange.max) :
                                customerDistribution == DistributionType.UNIFORM ?
                                    new UniformLongGenerator(customerRange.min, customerRange.max) :
                                    new ZipfianGenerator(customerRange.min, customerRange.max);

            this.keyGeneratorPerWorkloadType = new()
            {
                [TransactionType.PRICE_UPDATE] = sellerIdGenerator,
                [TransactionType.DELETE_PRODUCT] = sellerIdGenerator,
                [TransactionType.DASHBOARD] = sellerIdGenerator,
                [TransactionType.CUSTOMER_SESSION] = customerIdGenerator
            };

            this.customerStatusCache = new();

            this.logger = LoggerProxy.GetInstance("WorkloadEmitter");
        }

		public async Task<(DateTime startTime, DateTime finishTime)> Run()
		{
            logger.LogInformation("[WorkloadEmitter] Start with concurrency level {0}", concurrencyLevel);
            SetUpCustomerWorkerListener();
            SetUpSellerWorkerListener();
            SetUpDeliveryWorkerListener();

            DateTime startTime = DateTime.Now;

            int submitted = 0;
            while (submitted < concurrencyLevel)
            {
                SubmitTransaction();
                submitted++;
            }

            // signal the queue has been drained
            Shared.WaitHandle.Add(0);

            // logger.LogInformation("[WorkloadEmitter] Will start waiting for result at {0}", DateTime.Now.Millisecond);
            submitted = 0;
            while (IsRunning())
            {
              
                // wait for results
                // todo
                Shared.ResultQueue.Take();  

                // logger.LogInformation("[WorkloadEmitter] Received a result at {0}. Submitting a new transaction at", DateTime.Now.Millisecond);

                SubmitTransaction();
                submitted++;

                // if(submitted % concurrencyLevel == 0)
                if (submitted == concurrencyLevel)
                {                    
                    logger.LogInformation("submit concurrency level reached. waiting for more transactions");
                    // await Task.Delay(10000);
                    Shared.WaitHandle.Add(0);
                    submitted = 0;
                }                            
            }

            DateTime finishTime = DateTime.Now;

            await customerWorkerSubscription.UnsubscribeAsync();
            await sellerWorkerSubscription.UnsubscribeAsync();
            await deliveryWorkerSubscription.UnsubscribeAsync();

            return (startTime, finishTime);
        }

        private void SubmitTransaction()
        {
            long threadId = Environment.CurrentManagedThreadId;
            // this.logger.LogInformation("Thread ID {0} Submit transaction called", threadId);
            TransactionInput txId = Shared.Workload.Take();
            // this.logger.LogInformation("Transaction ID {0} Transaction type {0}", txId.tid, txId.type.ToString());
            Console.WriteLine("[Emitter] TID: {0} TransType {1}", txId.tid, txId.type.ToString());
            try
            {
                long grainID;

                switch (txId.type)
                {
                    //customer worker
                    case TransactionType.CUSTOMER_SESSION:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        // but make sure there is no active session for the customer. if so, pick another customer
                        while (this.customerStatusCache.ContainsKey(grainID) &&
                                customerStatusCache[grainID] == CustomerWorkerStatus.BROWSING)
                        {
                            grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        }
                        
                        // this.logger.LogInformation("Customer worker {0} defined!", grainID);
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.CustomerStreamId, grainID.ToString());
                        // todo
                        this.customerStatusCache[grainID] = CustomerWorkerStatus.BROWSING;
                        _ = streamOutgoing.OnNextAsync(txId.tid);
                        // this.logger.LogInformation("Customer worker {0} message sent!", grainID);
                        break;
                    }
                    // delivery worker
                    case TransactionType.UPDATE_DELIVERY:
                    {
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, "0");
                        _ = streamOutgoing.OnNextAsync(txId.tid);
                        break;
                    }
                    // seller worker
                    case TransactionType.DASHBOARD:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        break;
                    }
                    case TransactionType.PRICE_UPDATE:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        break;
                    }
                    // seller
                    case TransactionType.DELETE_PRODUCT:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        break;
                    }
                    default: { throw new Exception("Thread ID " + threadId + " Unknown transaction type defined!"); }
                }
            }
            catch (Exception e)
            {
                this.logger.LogError("Thread ID {0} Error caught in SubmitTransaction: {1}", threadId, e.Message);
            }

        }

        private Task UpdateCustomerStatusAsync(CustomerWorkerStatusUpdate update, StreamSequenceToken token = null)
        {
            Console.WriteLine("--- [Stream] (received in Emitter) Customer response <-- Kafka");
            var old = this.customerStatusCache[update.customerId];
            // this.logger.LogInformation("Attempt to update customer worker {0} status in cache. Previous {1} Update {2}",
            //     update.customerId, old, update.status);
            this.customerStatusCache[update.customerId] = update.status;
            // Shared.ResultQueue.Add(0);
            return Task.CompletedTask;
        }

        private async void SetUpCustomerWorkerListener()
        {
            IAsyncStream<CustomerWorkerStatusUpdate> resultStream = streamProvider.GetStream<CustomerWorkerStatusUpdate>(StreamingConstants.CustomerStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.customerWorkerSubscription = await resultStream.SubscribeAsync(UpdateCustomerStatusAsync);
        }

        private Task UpdateSellerStatusAsync(SellerWorkerStatusUpdate update, StreamSequenceToken token = null)
        {
            // Console.WriteLine("--- [Stream] (received in Emitter) Seller response <-- Kafka");
            // Shared.ResultQueue.Add(0);
            return Task.CompletedTask;
        }

        private Task UpdateDeliveryStatusAsync(int tid, StreamSequenceToken token = null)
        {            
            // Console.WriteLine("--- [Stream] (received in Emitter) Delievery response <-- Kafka");
            // Shared.ResultQueue.Add(0);
            return Task.CompletedTask;
        }

        private async void SetUpSellerWorkerListener()
        {            
            IAsyncStream<SellerWorkerStatusUpdate> resultStream = streamProvider.GetStream<SellerWorkerStatusUpdate>(StreamingConstants.SellerStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.sellerWorkerSubscription = await resultStream.SubscribeAsync(UpdateSellerStatusAsync);
        }

        private async void SetUpDeliveryWorkerListener()
        {
            IAsyncStream<int> resultStream = streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.deliveryWorkerSubscription = await resultStream.SubscribeAsync(UpdateDeliveryStatusAsync);
        }
    }
}