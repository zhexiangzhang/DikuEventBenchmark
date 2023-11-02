using Common.Infra;
using Common.Streaming;
using Common.Response;
using Confluent.Kafka;
using Client.Workload;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams;
using Newtonsoft.Json;


namespace Client.Streaming.Kafka
{
    public class KafkaConsumer : Stoppable
    {
        public const int defaultDelay = 5000;
        public static TimeSpan defaultBlockForConsume = TimeSpan.FromSeconds(2.5);
        private readonly Guid streamId;
        private readonly IConsumer<string, Event> consumer;
        private readonly IStreamProvider streamProvider;
        private readonly string kafkaTopic;
        private IAsyncStream<Event> streamForDelivery = null;
        private readonly Dictionary<string, IAsyncStream<Event>> streamCache;

        public KafkaConsumer(IConsumer<string, Event> consumer, IStreamProvider streamProvider, Guid streamId, string kafkaTopic)
        {
            this.consumer = consumer;
            this.streamProvider = streamProvider;
            this.kafkaTopic = kafkaTopic;
            this.streamId = streamId;
            this.streamCache = new Dictionary<string, IAsyncStream<Event>>();
            if (streamId == StreamingConstants.DeliveryReactStreamId) {
                this.streamForDelivery = this.streamProvider.GetStream<Event>(streamId, "0");
            }
            Console.WriteLine("[Kafka] (init) topic: {0}", kafkaTopic);
        }

        public async Task Run()
        {
            Console.WriteLine("[Kafka] (start to run) topic: {0}", kafkaTopic);
            this.consumer.Subscribe(kafkaTopic);
            ConsumeResult<string, Event> consumeResult;
            while (this.IsRunning())
            {
                // Console.WriteLine("[Kafka] Topic {0} Waiting for message..", kafkaTopic);
                
                consumeResult = consumer.Consume(defaultBlockForConsume);
                if(consumeResult == null || consumeResult.Message == null)
                {
                    await Task.Delay(defaultDelay);
                } 
                else
                {
                    // let the generator know it can generate new transaction
                    Shared.ResultQueue.Add(0);

                    var responseJson = consumeResult.Message.Value;
                    kafkaResponse response = JsonConvert.DeserializeObject<kafkaResponse>(responseJson.payload);
                    Console.WriteLine(" ----- [Kafka Consumer] received Topic {0}, tid {1}", kafkaTopic, response.tid);

                    if (this.kafkaTopic == "updateDeliveryTask")
                    {
                        _ = streamForDelivery.OnNextAsync(consumeResult.Message.Value);
                        // await Task.Run(() => streamForDelivery.OnNextAsync(consumeResult.Message.Value));
                    }
                    else
                    {
                        if (streamCache.ContainsKey(response.receiver))
                        {
                            _ = streamCache[response.receiver].OnNextAsync(consumeResult.Message.Value);
                        } 
                        else
                        {                                                    
                            // IAsyncStream<Event> stream = this.streamProvider.GetStream<Event>(streamId, consumeResult.Message.Key);                            
                            IAsyncStream<Event> stream = this.streamProvider.GetStream<Event>(streamId, response.receiver);
                            streamCache[response.receiver] = stream;
                            _ = stream.OnNextAsync(consumeResult.Message.Value);          
                        } 
                    }
                }
            }

        }

        public override void Dispose()
        {
            // https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Web/RequestTimeConsumer.cs
            this.consumer.Close();
            this.consumer.Dispose();
            base.Dispose();
        }
    }
}