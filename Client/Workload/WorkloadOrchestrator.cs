﻿using Orleans;
using System.Threading.Tasks;
using Common.Workload;
using Common.Infra;
using Microsoft.Extensions.Logging;
using System;
using Common.Distribution;
using System.Collections.Generic;

namespace Client.Workload
{
    public class WorkloadOrchestrator
    {
        private readonly static ILogger logger = LoggerProxy.GetInstance("WorkloadOrchestrator");

        public static async Task<(DateTime startTime, DateTime finishTime)> Run(IClusterClient orleansClient, IDictionary<TransactionType, int> transactionDistribution, int concurrencyLevel,
            DistributionType sellerDistribution, Interval sellerRange, DistributionType customerDistribution, Interval customerRange, int delayBetweenRequests, int executionTime)
        {
            logger.LogInformation("Workload orchestrator started.");

            WorkloadGenerator workloadGen = new WorkloadGenerator(
                transactionDistribution, concurrencyLevel);

            // makes sure there are transactions
            workloadGen.Prepare();

            Task genTask = Task.Run(workloadGen.Run);

            WorkloadEmitter emitter = new WorkloadEmitter(
                orleansClient,
                sellerDistribution,
                sellerRange,
                customerDistribution,
                customerRange,
                concurrencyLevel,
                delayBetweenRequests);

            Task<(DateTime startTime, DateTime finishTime)> emitTask = Task.Run(emitter.Run);

            await Task.Delay(executionTime);

            emitter.Stop();
            workloadGen.Stop();

            await Task.WhenAll(genTask, emitTask);

            logger.LogInformation("Workload orchestrator has finished.");

            return emitTask.Result;

        }

    }
}

