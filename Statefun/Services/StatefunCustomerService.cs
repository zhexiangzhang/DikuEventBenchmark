using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Services;
using Common.Workload.Metrics;
using Statefun.Workers;

namespace Statefun.Services
{
    public class StatefunCustomerService : ICustomerService
    {
        private readonly Dictionary<int, StatefunCustomerThread> customers;

        public StatefunCustomerService(Dictionary<int, StatefunCustomerThread> customers)
        {
            this.customers = customers;
        }

        public List<TransactionIdentifier> GetSubmittedTransactions(int customerId)
        {
            return customers[customerId].GetSubmittedTransactions();
        }

        public void Run(int customerId, int tid) => customers[customerId].Run(tid);

        public void AddFinishedTransaction(int customerId, TransactionOutput transactionOutput)
        {
            customers[customerId].AddFinishedTransaction(transactionOutput);
        }

        public List<TransactionOutput> GetFinishedTransactions(int customerId)
        {
            return customers[customerId].GetFinishedTransactions();
        } 
    
    }
}