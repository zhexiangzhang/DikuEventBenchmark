using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Streaming;
using Confluent.Kafka;

namespace Client.Streaming.Kafka
{
    public class PayloadDeserializer : IDeserializer<Event>
	{
        public Event Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext ctx)
        {
            if (isNull) return null;
            byte[] bytes = data.ToArray();
            return new Event( ctx.Topic, System.Text.Encoding.UTF8.GetString(bytes, 0, bytes.Length) );
        }
    }
}