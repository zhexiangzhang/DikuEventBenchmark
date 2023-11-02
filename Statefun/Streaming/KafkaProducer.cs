using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Statefun.Streaming
{
    public class KafkaProducer
    {
        private readonly IProducer<string, string> producer;
        private readonly string topicName;
        private Random random;

        public KafkaProducer(string bootstrapServers, string topicName)
        {            
            var config = new ProducerConfig { BootstrapServers = bootstrapServers };            
            this.producer = new ProducerBuilder<string, string>(config).Build();            
            this.topicName = topicName;   
            // random = new Random();         
            // Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
        }

        public async Task ProduceAsync(string key, string json, int kafkaPartition)
        {
            if (producer == null)
            {
                Console.WriteLine("producer is null");
            }
            // int partition = random.Next(10); 
            // var deliveryReport = await producer.ProduceAsync(new TopicPartition(topicName, partition), new Message<string, string> { Key = key, Value = json });

            // 
            // var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, string> { Key = key, Value = json });

            Console.WriteLine($"Message sent to partition: {kafkaPartition}, TOPIC: {this.topicName}");      
            var deliveryReport = await producer.ProduceAsync(new TopicPartition(topicName, kafkaPartition), new Message<string, string> { Key = key, Value = json });

            // Console.WriteLine($"Message sent to partition: {deliveryReport.Partition}, offset: {deliveryReport.Offset}");        
        }

        public void Stop()
        {
            // 确保所有的消息都被发送出去
            producer.Flush(TimeSpan.FromSeconds(10));
            // 释放生产者使用的所有资源
            producer.Dispose();
        }
    }
}