using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Client.Streaming.Kafka
{
    public class KafkaProducer
    {
        private readonly IProducer<string, string> producer;
        private readonly string topicName;

        public KafkaProducer(string bootstrapServers, string topicName)
        {
            var config = new ProducerConfig { BootstrapServers = bootstrapServers };
            this.producer = new ProducerBuilder<string, string>(config).Build();
            this.topicName = topicName;
            // 打印
            Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}. q to exit.");
        }

        public async Task ProduceAsync(string key, string json)
        {
            // try
            // {
                // Console.WriteLine($"Producing record: {key} {json}");
                if (producer == null)
                {
                    Console.WriteLine("producer is null");
                }
                var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, string> { Key = key, Value = json });

                // 打印发送结果
                Console.WriteLine($"Message sent to partition: {deliveryReport.Partition}, offset: {deliveryReport.Offset}");        
        }

         public void Stop()
        {
            // 确保所有的消息都被发送出去
            producer.Flush(TimeSpan.FromSeconds(10));
            // 释放生产者使用的所有资源
            producer.Dispose();
        }

        public void Test()
        {
            Console.WriteLine("KafkaProducer Test");
        }
    }
}





