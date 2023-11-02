using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Client.Streaming.Kafka
{
    public class KafkaConfig
    {
        public IDictionary<string, string> ingestTopics;
        public bool KafkaEnabled { get; set; }

        public string KafkaService { get; set; }

        public string DefaultStreamProvider { get; set; }
    }
}