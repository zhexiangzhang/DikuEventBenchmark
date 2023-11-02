using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GrainInterfaces.Workers;
using Orleans.Concurrency;
using Client.Streaming.Kafka;
using Orleans;
using Microsoft.Extensions.Logging;

namespace Grains.Workers
{

    [StatelessWorker]
    public class KafkaProducerWorker : Grain, IKafkaProducerWorker
    {
        private Dictionary<string, KafkaProducer> kafkaProducers;
        private readonly ILogger<KafkaProducerWorker> _logger;

        string kafkaService = "localhost:9092";

        public KafkaProducerWorker(ILogger<KafkaProducerWorker> logger)
        {
            this._logger = logger;
        }

        public override Task OnActivateAsync()
        {
            this.kafkaProducers = new Dictionary<string, KafkaProducer>();
            List<string> topics = new List<string>() 
            { 
                "priceUpdate", "productDelete", "queryDashboard",
                // "addToCart", "checkout", "clearCart",
                "customerSession",
                "updateDelivery" 
            };
            // 创建消费者
            foreach (var topic in topics)
            {
                KafkaProducer kafkaProducer = new KafkaProducer(kafkaService, topic);
                this.kafkaProducers.Add(topic, kafkaProducer);
            }
            return Task.CompletedTask;
            // create own instance of kafka producer            
        }

        public override Task OnDeactivateAsync()
        {
            // disconnect and dispose instance of kafka producer  
            foreach (var kafkaProducer in kafkaProducers.Values)
            {
                kafkaProducer.Stop();
            }          
            return Task.CompletedTask;
        }

        public async Task Publish(string topic, string key, string payload)
        {
            _logger.LogInformation($"Publishing to topic: {topic} with key: {key} and payload: {payload}");
            await this.kafkaProducers[topic].ProduceAsync(key, payload);
        }        
    }
}