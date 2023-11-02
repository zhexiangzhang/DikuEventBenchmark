using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Workload.Workers;
using Common.Workers;
using System.Threading.Channels;
using Common.Infra;
using Common.Workload;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using Statefun.Workload;
using Statefun.Streaming;

namespace Statefun.Workers
{

    public class StatefunDeliveryThread : IDeliveryWorker
    {
        private readonly DeliveryWorkerConfig config;

        private readonly ILogger logger;

        private readonly List<TransactionIdentifier> submittedTransactions;

        private readonly List<TransactionOutput> finishedTransactions;

        private readonly Random randomKafka;

        // private KafkaProducer kafkaProducer;

        public static StatefunDeliveryThread BuildDeliveryThread(DeliveryWorkerConfig config)
        {
            var logger = LoggerProxy.GetInstance("Delivery");
            return new StatefunDeliveryThread(config, logger);
        }

        private StatefunDeliveryThread(DeliveryWorkerConfig config, ILogger logger)
        {
            this.config = config;            
            this.logger = logger;
            this.submittedTransactions = new List<TransactionIdentifier>();
            this.finishedTransactions = new List<TransactionOutput>();
            this.randomKafka = new Random();
            // this.kafkaProducer = new KafkaProducer("kafkahost:9092", "updateDelivery");
        }
        
        public void Run(int tid)
        {
            updateDelivery(tid);
        }

        public async void updateDelivery(int tid) {

            JObject payloadObject = new JObject();
            payloadObject["tid"] = tid;
            string payload = JsonConvert.SerializeObject(payloadObject);         

            // int partitionID = 0; 
            int partitionID = randomKafka.Next(10);       
            int partitionkafka = randomKafka.Next(37);

            await StatefunShared.UpdateDeliveryMessages.Writer.WriteAsync(
                new KafkaTransactionMessage(
                        partitionID.ToString(),
                        payload,
                        partitionkafka
            )); 

            // await this.kafkaProducer.ProduceAsync(partitionID.ToString(), payload);

            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, DateTime.UtcNow));    
        }

        public void AddFinishedTransaction(TransactionOutput transactionOutput)
        {
            this.finishedTransactions.Add(transactionOutput);
        }

        public List<TransactionIdentifier> GetSubmittedTransactions()
        {
            return this.submittedTransactions;
        }

        public List<TransactionOutput> GetFinishedTransactions()
        {
            return this.finishedTransactions;
        }
    }
}