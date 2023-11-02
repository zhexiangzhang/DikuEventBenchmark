﻿using Orleans;
using System;
using System.Threading.Tasks;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using System.Collections.Generic;
using Client.Streaming.Kafka;

namespace GrainInterfaces.Workers
{
	public interface IDeliveryWorker : IGrainWithIntegerKey
    {
        public Task Init(DeliveryWorkerConfig config);

        public Task<List<Latency>> Collect(DateTime startTime);
	}
}

