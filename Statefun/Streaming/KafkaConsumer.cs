using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Statefun.Services;
using Confluent.Kafka;
using Newtonsoft.Json;
using Common.Streaming;
using Common.Workload;
using Common.Infra;
using System.Transactions;
using Common.Workload.Metrics;

namespace Statefun.Streaming
{
    public class KafkaConsumer : Stoppable
    {
        public const int defaultDelay = 5000;
        public static TimeSpan defaultBlockForConsume = TimeSpan.FromSeconds(2.5);
        // private readonly Guid streamId;
        private readonly IConsumer<string, Event> consumer;
        // private readonly IStreamProvider streamProvider;
        private readonly string kafkaTopic;

        private bool running = false;
        private readonly StatefunSellerService sellerService;
        private readonly StatefunCustomerService customerService;
        private readonly StatefunDeliveryService deliveryService;

        public KafkaConsumer(IConsumer<string, Event> consumer, string kafkaTopic, 
        StatefunSellerService sellerService, StatefunCustomerService customerService, StatefunDeliveryService delivery)
        {
            this.consumer = consumer;
            this.kafkaTopic = kafkaTopic;
            this.sellerService = sellerService;
            this.customerService = customerService;
            this.deliveryService = delivery;
            Console.WriteLine("[Kafka] (init) topic: {0}", kafkaTopic);
        }

        public async Task Run(CancellationToken cancellationToken){

            int threadId = Thread.CurrentThread.ManagedThreadId;
            int[] partitionCount = new int[40];

            Console.WriteLine("[Kafka] (start to run) topic: {0}, thread ID {1}", kafkaTopic, threadId);
            
            this.consumer.Subscribe(kafkaTopic);
            ConsumeResult<string, Event> consumeResult;
            this.running = true;
            // while(this.IsRunning())
            while(!cancellationToken.IsCancellationRequested)
            {
                consumeResult = consumer.Consume(defaultBlockForConsume);
                
                if (consumeResult == null || consumeResult.Message == null)
                {
                    await Task.Delay(defaultDelay);
                    Console.WriteLine("// --- No message Kafka --- //"); 
                    if (this.running == false) {
                        break;
                    }
                } 
                else
                {
                    // Console.WriteLine(" Yes kafka receive a message ................");  
                    DateTime now = DateTime.UtcNow;
                    // let the generator know it can generate new transaction
                    await Shared.ResultQueue.Writer.WriteAsync(WorkloadManager.ITEM);

                    // put the result into different result queue

                    int partitionInfo = consumeResult.Partition.Value;
                    partitionCount[partitionInfo] += 1;

                    var responseJson = consumeResult.Message.Value;
                    kafkaResponse response = JsonConvert.DeserializeObject<kafkaResponse>(responseJson.payload);
                    TransactionOutput transactionOutput = new TransactionOutput(response.tid, now);   


                    // Console.WriteLine(" === [Kafka] === topic: {0} receive a message", kafkaTopic);                    
                    // Console.WriteLine(" ----- (Tid - " + response.tid + ") Receive topic " + kafkaTopic); 
                    Console.WriteLine("// --- kafka receive tid : " + response.tid + "  " + kafkaTopic + " --- // partitionInfo: " + partitionInfo);  
                    // Console.WriteLine("[---->]  [Tid: {0}]  [{1}}]  [Result:{2}]", response.tid, kafkaTopic, response.status); 

                    switch(this.kafkaTopic)
                    {
                        case "checkoutTask":        
                            customerService.AddFinishedTransaction(response.receiver, transactionOutput);             
                            if (response.status == MarkStatus.SUCCESS)
                                await Shared.CheckoutOutputs.Writer.WriteAsync(transactionOutput);
                            else
                                await Shared.PoisonCheckoutOutputs.Writer.WriteAsync(
                                    new TransactionMark(response.tid, TransactionType.CUSTOMER_SESSION, response.receiver, response.status, response.source ));                                                            
                            break;

                        case "updateProductTask":
                            sellerService.AddFinishedTransaction(response.receiver, transactionOutput);
                            if (response.status == MarkStatus.SUCCESS)
                                await Shared.ProductUpdateOutputs.Writer.WriteAsync(transactionOutput);
                            else
                                await Shared.PoisonProductUpdateOutputs.Writer.WriteAsync(
                                    new TransactionMark(response.tid, TransactionType.UPDATE_PRODUCT, response.receiver, response.status, response.source ));
                            break;

                        case "updatePriceTask":
                            sellerService.AddFinishedTransaction(response.receiver, transactionOutput);
                            if (response.status == MarkStatus.SUCCESS)
                                await Shared.PriceUpdateOutputs.Writer.WriteAsync(transactionOutput);
                            else
                                await Shared.PoisonPriceUpdateOutputs.Writer.WriteAsync(
                                    new TransactionMark(response.tid, TransactionType.PRICE_UPDATE, response.receiver, response.status, response.source ));
                            break;
                        
                        case "queryDashboardTask":
                            sellerService.AddFinishedTransaction(response.receiver, transactionOutput);
                            if (response.status == MarkStatus.SUCCESS)
                                await Shared.DashboardQueryOutputs.Writer.WriteAsync(transactionOutput);
                            else
                                await Shared.PoisonDashboardQueryOutputs.Writer.WriteAsync(
                                    new TransactionMark(response.tid, TransactionType.QUERY_DASHBOARD, response.receiver, response.status, response.source ));
                            break;

                        case "updateDeliveryTask":
                            deliveryService.AddFinishedTransaction(transactionOutput);
                            if (response.status == MarkStatus.SUCCESS)
                                await Shared.DeliveryUpdateOutputs.Writer.WriteAsync(transactionOutput);
                            else
                                await Shared.PoisonDeliveryUpdateOutputs.Writer.WriteAsync(
                                    new TransactionMark(response.tid, TransactionType.UPDATE_DELIVERY, response.receiver, response.status, response.source ));
                            break;

                        default:
                            throw new Exception("Unknown topic");
                    }                    
                }
            }

            Console.WriteLine("[Kafka] (stop) topic: {0}, partition count: {1}", kafkaTopic, string.Join(",", partitionCount));

        }

        public override void Dispose()
        {
            // https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Web/RequestTimeConsumer.cs
            Console.WriteLine("consumer stop");  
            this.running = false;
            this.consumer.Close();
            this.consumer.Dispose();
            base.Dispose();
        }
    }


}