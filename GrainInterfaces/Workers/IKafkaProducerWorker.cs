using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Common.Workload.Delivery;
using Common.Workload.Metrics;

namespace GrainInterfaces.Workers
{
    public interface IKafkaProducerWorker: IGrainWithIntegerKey
    {
        public Task Publish(string topic, string key, string payload);
    }
}