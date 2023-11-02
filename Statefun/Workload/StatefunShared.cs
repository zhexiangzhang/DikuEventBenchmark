using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Channels;
using Statefun.Streaming;
using Common.Streaming;
using Common.Workload.Metrics;

namespace Statefun.Workload
{
    public sealed class StatefunShared
    {    
        public static readonly Channel<KafkaTransactionMessage> PriceUpdateMessages = Channel.CreateUnbounded<KafkaTransactionMessage>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<KafkaTransactionMessage> ProductUpdateMessages = Channel.CreateUnbounded<KafkaTransactionMessage>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<KafkaTransactionMessage> QueryDashboardMessages = Channel.CreateUnbounded<KafkaTransactionMessage>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<KafkaTransactionMessage> CustomerSessionMessages = Channel.CreateUnbounded<KafkaTransactionMessage>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<KafkaTransactionMessage> UpdateDeliveryMessages = Channel.CreateUnbounded<KafkaTransactionMessage>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
    }
}