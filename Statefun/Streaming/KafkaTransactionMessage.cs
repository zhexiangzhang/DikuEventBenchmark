using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Statefun.Streaming
{
    public record KafkaTransactionMessage
	(
		string key,
		string payload,
		int kafkaPartition
	);
}