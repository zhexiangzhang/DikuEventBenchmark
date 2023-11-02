﻿using System;
using System.Collections.Generic;
using Common;
using Common.Workload;
using Common.Workload.Customer;
using Common.Workload.Seller;
using Client.Streaming;
using Common.Distribution;
using Common.Streaming;
using Common.Workload.Delivery;

namespace Client.Workload
{

    public class WorkloadConfig
    {

        // maximum number of concurrent transactions submitted to the target system
        public int concurrencyLevel { get; set; }

        // a timer is configured to notify the orchestrator grain about the termination
        public int executionTime { get; set; } = 6000;

        public int delayBetweenRequests = 1000;

        // client perspective
        public bool endToEndLatencyCollection = false;

        /**
         * from highest to lowest. last entry must be 100
         * e.g. customer_session 70, price_update 95, delivery 100
         * that means 70% of transactions are customer sessions
         * 25% price update and 5% delivery
         */
        public IDictionary<TransactionType,int> transactionDistribution { get; set; }

        // map kafka topic to orleans stream Guid
        public readonly Dictionary<string, Guid> mapTopicToStreamGuid = new()
        {
            // ["addItemToCartTask"] = StreamingConstants.CustomerReactStreamId,
            ["checkoutTask"] = StreamingConstants.CustomerReactStreamId,
            ["deleteProductTask"] = StreamingConstants.SellerReactStreamId,
            ["updatePriceTask"] = StreamingConstants.SellerReactStreamId,
            ["updateDeliveryTask"] = StreamingConstants.DeliveryReactStreamId,
            ["queryDashboardTask"] = StreamingConstants.SellerReactStreamId
        };

        public StreamingConfig streamingConfig { get; set; }

        public CustomerWorkerConfig customerWorkerConfig { get; set; }

        public SellerWorkerConfig sellerWorkerConfig { get; set; }

        public DeliveryWorkerConfig deliveryWorkerConfig { get; set; }

        // customer key distribution
        public DistributionType customerDistribution { get; set; }

        // public StateFunConfig stateFunConfig { get; set; }
    }
}
