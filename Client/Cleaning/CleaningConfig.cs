﻿using System.Collections.Generic;
using Client.Streaming;

namespace Client.Cleaning
{
	public class CleaningConfig
	{
        public StreamingConfig streamingConfig { get; set; }

        public IDictionary<string, string> mapMicroserviceToUrl { get; set; }

        public const string cleanupEndpoint = "/cleanup";

    }
}

