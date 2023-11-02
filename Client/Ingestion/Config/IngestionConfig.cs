using System;
using System.Collections.Generic;

namespace Client.Ingestion.Config
{
    public class IngestionConfig
    {

        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        // distribution of work strategy
        public IngestionDistributionStrategy distributionStrategy = IngestionDistributionStrategy.SINGLE_WORKER;

        // number of logical processors = Environment.ProcessorCount
        public int numberCpus = Environment.ProcessorCount;

        public IDictionary<string, string> mapTableToUrl;

        public TargetPlatform targetPlatform = TargetPlatform.STATEFUN;

        public int productPartion { get; set; }

        public int sellerPartion { get; set; }

        public int customerPartion { get; set; }

        public int stockPartion { get; set; }

    }

}
