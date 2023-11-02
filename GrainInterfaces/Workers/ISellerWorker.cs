using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Entities;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Orleans;
using Orleans.Concurrency;
using Client.Streaming.Kafka;

namespace GrainInterfaces.Workers
{
	public interface ISellerWorker : IGrainWithIntegerKey
	{
        public Task Init(SellerWorkerConfig sellerConfig, List<Product> products, bool endToEndLatencyCollection, string connection);

        [AlwaysInterleave]
        public Task<long> GetProductId();

        public Task<Product> GetProduct();

        public Task<List<Latency>> Collect(DateTime startTime);

    }
}
