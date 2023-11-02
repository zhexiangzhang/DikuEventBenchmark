using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Infra;
using Statefun.Workload;
using System.Threading.Channels;

namespace Statefun.Streaming
{
    public class KafkaProducerThread
    {
        private readonly ILogger logger;

        private String kafkaService;

        private String topicName;

        private KafkaProducer kafkaProducer;        

        private bool isRunning = false;

        private readonly Channel<KafkaTransactionMessage> massageChannel;

        public static KafkaProducerThread BuildKafkaProducerThread(String kafkaService, String topicName)
        {
            var logger = LoggerProxy.GetInstance("KafkaProducer" + topicName);
            return new KafkaProducerThread(kafkaService, topicName, logger);
        }
        
        private KafkaProducerThread(String kafkaService, String topicName, ILogger logger)
        {
            this.kafkaService = kafkaService;
            this.topicName = topicName;     
            this.logger = logger;
            this.kafkaProducer = new KafkaProducer(kafkaService, topicName);
            this.isRunning = true;

            this.massageChannel = SetUpChannel();
        }

        private Channel<KafkaTransactionMessage> SetUpChannel()
        {
            if (topicName == "priceUpdate")
            {
                return StatefunShared.PriceUpdateMessages;
            }
            else if (topicName == "productUpdate")
            {
                return StatefunShared.ProductUpdateMessages;
            }
            else if (topicName == "queryDashboard")
            {
                return StatefunShared.QueryDashboardMessages;
            }
            else if (topicName == "customerSession")
            {
                return StatefunShared.CustomerSessionMessages;
            }
            else if (topicName == "updateDelivery")
            {
                return StatefunShared.UpdateDeliveryMessages;
            }
            else
            {
                throw new Exception("Unknown topic name: " + topicName);
            }
        }

        // todo 不知道合不合理
        public async void Start()
        {
            Console.WriteLine("start reading");            
            // while (isRunning)
            // {                                
            //     if (this.massageChannel.Reader.TryRead(out _))
            //     {
            //         Console.WriteLine(topicName + " one data reading");    
            //     }            
            // }
            // while (isRunning)
            // {
            //     // while (this.massageChannel.Reader.TryRead(out KafkaTransactionMessage item)) {
            //     //     Console.WriteLine(item);       
            //     // }
                
            //     // this.Publish(item.key, item.payload);
            // }

            while (isRunning) {
                if (await this.massageChannel.Reader.WaitToReadAsync())
                {
                    if (this.massageChannel.Reader.TryRead(out KafkaTransactionMessage item))
                    {                        
                        this.Publish(item.key, item.payload, item.kafkaPartition);
                        Console.WriteLine(topicName + " one data publishing");
                    }
                }
            }

            // while (StatefunShared.CheckoutOutputs.Reader.TryRead(out TransactionOutput item))
        }

        public async void Publish(string key, string payload, int kafkaPartition)
        {
            // Console.WriteLine("Publishing to topic: " + this.topicName + " key: " + key);                        
            await this.kafkaProducer.ProduceAsync(key, payload, kafkaPartition);
        }

        public void Stop()
        {
            this.isRunning = false;
            this.kafkaProducer.Stop();
        }
    }
}