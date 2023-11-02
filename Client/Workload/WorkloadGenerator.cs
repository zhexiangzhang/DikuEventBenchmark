﻿using System;
using Common.Workload;
using Common.Workload.Seller;
using System.Collections.Generic;
using System.Linq;
using Common.Infra;
using Microsoft.Extensions.Logging;

namespace Client.Workload
{
	public class WorkloadGenerator : Stoppable
	{
        public readonly List<KeyValuePair<TransactionType, int>> workloadDistribution;

        private int tid = 1;
        private readonly int concurrencyLevel;

        private readonly Random random;
        private readonly ILogger logger;

        public WorkloadGenerator(IDictionary<TransactionType, int> workloadDistribution, int concurrencyLevel) : base()
        {
			this.concurrencyLevel = concurrencyLevel;
            this.workloadDistribution = workloadDistribution.ToList();
            this.random = new Random();
            this.logger = LoggerProxy.GetInstance("WorkloadGenerator");
        }

		public void Run()
		{
            
            int initialNumTxs = concurrencyLevel + (int)(concurrencyLevel * 0.25);

            // TODO keep an histogram in memory so we can see whether the distibution is correct
            Generate(initialNumTxs);

            while (IsRunning())
            {
                //logger.LogInformation("[WorkloadGenerator] Will wait for signal at {0}", DateTime.Now.Millisecond);
                // wait for queue to be exhausted enough
                Shared.WaitHandle.Take();
                // logger.LogInformation("[WorkloadGenerator] Received signal at {0}", DateTime.Now.Millisecond);
                // logger.LogInformation("[WorkloadGenerator] generating new transactions");
                // 睡眠 1 秒

                Generate(concurrencyLevel);
            }
			
		}

        private void Generate(int num)
        {
            for (int i = 0; i < num; i++)
            {
                TransactionType tx = PickTransactionFromDistribution();
                Shared.Workload.Add(new TransactionInput(tid,tx));
                tid++;
            }
        }

        private TransactionType PickTransactionFromDistribution()
        {
            int x = random.Next(0, 101);
            foreach (var entry in workloadDistribution)
            {
                if (x <= entry.Value)
                {
                    return entry.Key;
                }
            }
            throw new Exception("Cannot find a transaction to select!");
        }

    }
}

