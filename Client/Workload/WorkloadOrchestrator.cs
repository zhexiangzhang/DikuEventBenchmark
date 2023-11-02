using Orleans;
using System;
using System.Threading.Tasks;
using Common.Workload;
using System.Collections.Generic;
using Common.Infra;
using Microsoft.Extensions.Logging;
using System.Linq;
using Client.Streaming.Redis;

namespace Client.Workload
{

    public class WorkloadOrchestrator
    {
        private readonly IClusterClient orleansClient;
        private readonly WorkloadConfig workloadConfig;
        private readonly Interval customerRange;
        private readonly ILogger logger;

        public WorkloadOrchestrator(IClusterClient orleansClient, WorkloadConfig workloadConfig, Interval customerRange)
        {
            this.orleansClient = orleansClient;
            this.workloadConfig = workloadConfig;
            this.customerRange = customerRange;
            this.logger = LoggerProxy.GetInstance("WorkloadOrchestrator");
        }

        public async Task<(DateTime startTime, DateTime finishTime)> Run()
        {
            logger.LogInformation("Workload orchestrator started.");

            // clean streams beforehand. make sure microservices do not receive events from previous runs
            List<string> channelsToTrim = workloadConfig.streamingConfig.streams.ToList();

            WorkloadGenerator workloadGen = new WorkloadGenerator(this.workloadConfig.transactionDistribution, this.workloadConfig.concurrencyLevel);

            Task genTask = Task.Run(workloadGen.Run);

            WorkloadEmitter emitter = new WorkloadEmitter(
                orleansClient,
                workloadConfig.customerWorkerConfig.sellerDistribution,
                workloadConfig.customerWorkerConfig.sellerRange,
                workloadConfig.customerDistribution,
                customerRange,
                workloadConfig.concurrencyLevel);           
            
            Task<(DateTime startTime, DateTime finishTime)> emitTask = Task.Run(emitter.Run);

            await Task.Delay(this.workloadConfig.executionTime);
            
            workloadGen.Stop();
            emitter.Stop();

            logger.LogInformation("Workload orchestrator has finished.");

            return emitTask.Result;

        }

    }
}

