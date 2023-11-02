using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Entities;
using Common.Services;
using Common.Workload;
using Common.Workload.Metrics;
using Statefun.Workers;
using Common.Workers;

namespace Statefun.Services
{
    public class StatefunSellerService : ISellerService
    {
        private readonly Dictionary<int, StatefunSellerThread> sellers;

        public StatefunSellerService(Dictionary<int, StatefunSellerThread> sellers)
        {
            this.sellers = sellers;
        }

        public List<TransactionOutput> GetFinishedTransactions(int sellerId)
        {
            return sellers[sellerId].GetFinishedTransactions();
        }

        public Product GetProduct(int sellerId, int idx) => sellers[sellerId].GetProduct(idx);

        public List<TransactionIdentifier> GetSubmittedTransactions(int sellerId)
        {
            return sellers[sellerId].GetSubmittedTransactions();
        }

        public void Run(int sellerId, int tid, TransactionType type) => sellers[sellerId].Run(tid, type);            

        public void AddFinishedTransaction(int sellerId, TransactionOutput transactionOutput)
        {
            this.sellers[sellerId].AddFinishedTransaction(transactionOutput);
        }
    }
}