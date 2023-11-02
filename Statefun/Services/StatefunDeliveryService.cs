using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Services;
using Common.Workload.Metrics;
using Statefun.Workers;

namespace Statefun.Services 
{
    public class StatefunDeliveryService : IDeliveryService
    {
        private StatefunDeliveryThread deliveryThread;

        public StatefunDeliveryService(StatefunDeliveryThread deliveryThread)
        {
            this.deliveryThread = deliveryThread;
        }
        
        public List<(TransactionIdentifier, TransactionOutput)> GetResults()
        {
            // return deliveryThread.GetResults();
            throw new NotImplementedException();
        }

        public List<TransactionIdentifier> GetSubmittedTransactions()
        {
            return deliveryThread.GetSubmittedTransactions();
        }

        
        public List<TransactionOutput> GetFinishedTransactions()
        {
            return deliveryThread.GetFinishedTransactions();
        } 

        public void Run(int tid)
        {
            deliveryThread.Run(tid);
        }

        public void AddFinishedTransaction(TransactionOutput transactionOutput)
        {
            this.deliveryThread.AddFinishedTransaction(transactionOutput);
        }
    }
}