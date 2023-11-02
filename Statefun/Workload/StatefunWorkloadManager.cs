using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Services;
using Common.Workload;

namespace Statefun.Workload
{
    public class StatefunWorkloadManager : WorkloadManager
    {
        private readonly ISellerService sellerService;
        private readonly ICustomerService customerService;
        private readonly IDeliveryService deliveryService;

        public StatefunWorkloadManager(
            ISellerService sellerService,
            ICustomerService customerService,
            IDeliveryService deliveryService,
            IDictionary<TransactionType, int> transactionDistribution, 
            Interval customerRange, 
            int concurrencyLevel, int executionTime, int delayBetweenRequests) :
            base(transactionDistribution, customerRange, concurrencyLevel, executionTime, delayBetweenRequests)
        {
            this.sellerService = sellerService;
            this.customerService = customerService;
            this.deliveryService = deliveryService;
        }

        protected override void SubmitTransaction(int tid, TransactionType type)
        {
            try 
            {
                switch (type)
                {
                    case TransactionType.CUSTOMER_SESSION:
                        {
                            int customerId;
                            // Console.WriteLine(" --------- customerIdleQueue.Count: " + this.customerIdleQueue.Count + "---------------");
                            while (!this.customerIdleQueue.TryDequeue(out customerId)) { }

                            Task.Run(() => customerService.Run(customerId, tid)).ContinueWith(x => this.customerIdleQueue.Enqueue(customerId));
                            break;
                        }
                    // delivery worker
                    case TransactionType.UPDATE_DELIVERY:
                        {
                            Task.Run(() => deliveryService.Run(tid));                            
                            break;
                        }
                    // seller worker
                    case TransactionType.QUERY_DASHBOARD:
                    case TransactionType.PRICE_UPDATE:
                    case TransactionType.UPDATE_PRODUCT:
                        {
                            int sellerId = this.sellerIdGenerator.Sample();
                            Task.Run(() => sellerService.Run(sellerId, tid, type));
                            break;
                        }
                    default:
                        long threadId = Environment.CurrentManagedThreadId;
                        this.logger.LogError("Thread ID " + threadId + " Unknown transaction type defined!");
                        break;
                }
            }
            catch (Exception e)
            {
                long threadId = Environment.CurrentManagedThreadId;
                this.logger.LogError("Thread ID {0} Error caught in SubmitTransaction: {1}", threadId, e.Message);
            }
        }
    }
}