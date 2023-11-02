using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Common.Response
{
    public class kafkaResponse
    {
        public long taskId { get; set; }
        public int tid { get; set; }
        public string receiver { get; set; }
        public string result { get; set; }
    }
}