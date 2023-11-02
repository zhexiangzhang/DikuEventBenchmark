using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Common.Entities;
using Common.Workload.Customer;
using Common.Workload.Metrics;
using Client.Streaming.Kafka;
using Orleans;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        Task Init(CustomerWorkerConfig config, Customer customer, bool endToEndLatencyCollection, string connection);

        public Task<List<Latency>> Collect(DateTime startTime);
    }
}
