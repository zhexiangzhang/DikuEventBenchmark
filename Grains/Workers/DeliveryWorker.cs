using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using GrainInterfaces.Workers;
using Common.Streaming;
using Orleans;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Common.Http;
using System.Net.Http;
using Orleans.Concurrency;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Response;
using System.Collections.Concurrent;
using Common.Workload.Delivery;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Client.Streaming.Kafka;

namespace Grains.Workers
{
    [Reentrant]
    public class DeliveryWorker : Grain, IDeliveryWorker
    {
        private DeliveryWorkerConfig config;

        private IStreamProvider streamProvider;

        private IAsyncStream<Event> kafkaStream;

        Dictionary<string, KafkaProducer> kafkaProducers;

        private readonly ILogger<DeliveryWorker> _logger;

        private long actorId;

        private IAsyncStream<int> txStream;
        private static Random rng = new Random();

        private readonly IDictionary<long, TransactionIdentifier> submittedTransactions;
        private readonly IDictionary<long, TransactionOutput> finishedTransactions;

        readonly String StateFunNamespace = "/e-commerce.fns/";
        readonly String StateFunHttpContentType = "application/vnd.e-commerce.types/";

        public DeliveryWorker(ILogger<DeliveryWorker> logger)
        {
            this._logger = logger;
            this.submittedTransactions = new ConcurrentDictionary<long, TransactionIdentifier>();
            this.finishedTransactions = new ConcurrentDictionary<long, TransactionOutput>();
        }

        public Task Init(DeliveryWorkerConfig config)
        {
            this.config = config;
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync()
        {
            this.actorId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            this.kafkaStream = streamProvider.GetStream<Event>(StreamingConstants.DeliveryReactStreamId, "0");

            var subscriptionHandles = await kafkaStream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ReactToKafkaResponse);
                }
            }
            await kafkaStream.SubscribeAsync<Event>(ReactToKafkaResponse);

            var workloadStream = streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, actorId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync<int>(Run);
        }

        // updating the delivery status of orders
        public async Task Run(int tid, StreamSequenceToken token)
        {
            // await Task.Run(async () => {
                // this._logger.LogWarning("Delivery {0}: Task started", this.actorId);
                // Console.WriteLine("[Delivery worker] {0}: Task started", this.actorId);

                try
                {
                    if (config.targetPlatform !=  Common.TargetPlatform.STATEFUN) { 
                        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, config.shipmentUrl + "/" + tid);
                        this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, DateTime.Now));
                        var resp = HttpUtils.client.Send(message);
                        if (resp.IsSuccessStatusCode)
                        {
                            this.finishedTransactions.Add(tid, new TransactionOutput(tid, DateTime.Now));
                        }
                        resp.EnsureSuccessStatusCode();
                    } else {    
                        // string url = config.shipmentUrl + StateFunNamespace + "shipmentProxy" + "/" + config.proxyAddress;
                        // string contentType = StateFunHttpContentType + "UpdateShipments";    
                                
                        JObject payloadObject = new JObject();
                        payloadObject["tid"] = tid;
                        string payload = JsonConvert.SerializeObject(payloadObject);                                                                                      

                        this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, DateTime.Now));
                        Console.WriteLine("[Delivery worker {0} | Tid {1}] : HTTP request sent", this.actorId, tid);
                        // await HttpUtils.SendHttpToStatefun(url, contentType, payload);                         

                        // string testLoad = "{\"stockItem\":{\"seller_id\":15,\"product_id\":40,\"qty_available\":9683,\"qty_reserved\":0,\"order_count\":0,\"ytd\":78,\"data\":\"Molestiae odio nihil sit fugiat quas.\"}}";
                        // await this.kafkaProducers["product"].ProduceAsync("1", testLoad);                
                        
                        int randomNumber = rng.Next(0, 10);
                 
                        var kafkaProducerWorker = GrainFactory.GetGrain<IKafkaProducerWorker>(0);
                        await kafkaProducerWorker.Publish("updateDelivery", config.proxyAddress.ToString(), payload);
                        // await this.kafkaProducersForTrans["updateDelivery"].ProduceAsync(config.proxyAddress.ToString(), payload);                   
                    }
                }
                catch(Exception e)
                {
                    this._logger.LogError("Delivery {0}: Update shipments could not be performed: {1}", this.actorId, e.Message);
                }
                // this._logger.LogWarning("Delivery {0}: task terminated!", this.actorId);
                // Console.WriteLine("[Delivery worker {0} | Tid {1}] : task terminated!", this.actorId, tid);
            // });
        }

        public Task<List<Latency>> Collect(DateTime startTime)
        {
            var targetValues = submittedTransactions.Values.Where(e => e.startTs.CompareTo(startTime) >= 0);
            var latencyList = new List<Latency>(submittedTransactions.Count());
            foreach (var entry in targetValues)
            {
                if (finishedTransactions.ContainsKey(entry.tid)) {
                    var res = finishedTransactions[entry.tid];
                    latencyList.Add(new Latency(entry.tid, entry.type,
                        (res.timestamp - entry.startTs).TotalMilliseconds ));
                }
            }
            return Task.FromResult(latencyList);
        }

        private Task ReactToKafkaResponse(Event responseEvent, StreamSequenceToken token)
        {
            kafkaResponse response = JsonConvert.DeserializeObject<kafkaResponse>(responseEvent.payload);
            if (responseEvent.topic == "updateDeliveryTask") 
            {
                this.finishedTransactions.Add(response.tid, new TransactionOutput(response.tid, DateTime.Now));
                Console.WriteLine(" ^-^ [Delivery worker {0} | Tid {1}] : Kafka received", this.actorId, response.tid);
                // this._logger.LogWarning("(+++ Kafka +++) task:{0} -- transactionID:{1} -- taskId:{2} -- success:{3}",responseEvent.topic, response.tid, response.taskId, response.result);
                // _ = txStream.OnNextAsync(response.tid);
            } 
            else 
            {
                this._logger.LogWarning("(+++ Kafka +++) task:{0} Unknow topic" + responseEvent.topic);
                // throw new Exception("Unknown topic: " + responseEvent.topic);
            }
            return Task.CompletedTask;
        }

    }
}

