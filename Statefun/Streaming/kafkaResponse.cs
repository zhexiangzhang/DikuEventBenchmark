using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Streaming;

namespace Statefun.Streaming
{
    public class kafkaResponse
    {
        public long taskId { get; set; }
        public int tid { get; set; }
        public int receiver { get; set; }
        // public string result { get; set; }
        public MarkStatus status { get; set; }

        public String source { get; set; }
    }
}