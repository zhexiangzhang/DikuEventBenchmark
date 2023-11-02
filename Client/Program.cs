﻿using Client.DataGeneration;
using Client.Workflow;
using Client.Collection;
using Client.Infra;
using Client.Streaming.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Extensions.Logging;
using System.Linq;
using Client.Ingestion.Config;
using Client.Workload;
using Common.Infra;

namespace Client
{

    public record MasterConfig(WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig, WorkloadConfig workloadConfig, KafkaConfig kafkaConfig, CollectionConfig collectionConfig);

    /**
     * Main method based on 
     * http://sergeybykov.github.io/orleans/1.5/Documentation/Deployment-and-Operations/Docker-Deployment.html
     */
    public class Program
    {
        private static ILogger logger = LoggerProxy.GetInstance("Program");

        /*
         * 
         */
        public static async Task Main(string[] args)
        {

            var masterConfig = BuildMasterConfig(args);

            logger.LogInformation("Initializing Orleans client...");
            var client = await OrleansClientFactory.Connect();
            if (client == null) return;
            logger.LogInformation("Orleans client initialized!");

            WorkflowOrchestrator orchestrator = new WorkflowOrchestrator(client, masterConfig.workflowConfig, masterConfig.syntheticDataConfig, masterConfig.ingestionConfig, masterConfig.workloadConfig, masterConfig.kafkaConfig, masterConfig.collectionConfig);
            await orchestrator.Run();

            logger.LogInformation("Workflow orchestrator finished!");

            try {
                await client.Close();
            } catch (Exception e) {
                logger.LogError("Error closing Orleans client: {0}", e.Message);
            }
            // await client.Close();

            logger.LogInformation("Orleans client finalized!");
        }

        /**
         * 1 - process directory of config files in the args. if not, select the default config files
         * 2 - parse config files
         * 3 - if files parsed are correct, then start orleans
         */
        public static MasterConfig BuildMasterConfig(string[] args)
        {
            
            logger.LogInformation("Initializing benchmark driver...");

            string initDir = Directory.GetCurrentDirectory();
            string configFilesDir;
            if (args is not null && args.Length > 0){
                configFilesDir = args[0];
                logger.LogInformation("Directory of configuration files passsed as parameter: {0}", configFilesDir);
            } else
            {
                configFilesDir = initDir;
            }
            Environment.CurrentDirectory = (configFilesDir);

            if (!File.Exists("workflow_config.json"))
            {
                throw new Exception("Workflow configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("data_load_config.json"))
            {
                throw new Exception("Data load configuration file cannot be loaded from "+ configFilesDir);
            }
            if (!File.Exists("ingestion_config.json"))
            {
                throw new Exception("Ingestion configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("workload_config.json"))
            {
                throw new Exception("Workload configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("kafka_config.json"))
            {
                throw new Exception("Kafka configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("collection_config.json"))
            {
                throw new Exception("Collection of metrics configuration file cannot be loaded from " + configFilesDir);
            }

            /** =============== Workflow config file ================= */
            logger.LogInformation("Init reading workflow configuration file...");
            WorkflowConfig workflowConfig;
            using (StreamReader r = new StreamReader("workflow_config.json"))
            {
                string json = r.ReadToEnd();
                logger.LogInformation("workflow_config.json contents:\n {0}", json);
                workflowConfig = JsonConvert.DeserializeObject<WorkflowConfig>(json);
            }
            logger.LogInformation("Workflow configuration file read succesfully");

            /** =============== Data load config file ================= */
            
            SyntheticDataSourceConfig dataLoadConfig = null;
            if (workflowConfig.dataLoad)
            {
                logger.LogInformation("Init reading data load configuration file...");
                using (StreamReader r = new StreamReader("data_load_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("data_load_config.json contents:\n {0}", json);
                    dataLoadConfig = JsonConvert.DeserializeObject<SyntheticDataSourceConfig>(json);
                }
                logger.LogInformation("Data load configuration file read succesfully");
            }

            /** =============== Ingestion config file ================= */
            IngestionConfig ingestionConfig = null;
            if (workflowConfig.ingestion)
            {
                logger.LogInformation("Init reading ingestion configuration file...");
                using (StreamReader r = new StreamReader("ingestion_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("ingestion_config.json contents:\n {0}", json);
                    ingestionConfig = JsonConvert.DeserializeObject<IngestionConfig>(json);
                }
                logger.LogInformation("Ingestion configuration file read succesfully");
            }

            /** =============== kafka config file ================= */
            KafkaConfig kafkaConfig = null;
            // if (workflowConfig.kafkaEnabled)
            // {
            logger.LogInformation("Init reading kafka configuration file...");
            using (StreamReader r = new StreamReader("kafka_config.json"))
            {
                string json = r.ReadToEnd();
                logger.LogInformation("kafka_config.json contents:\n {0}", json);
                kafkaConfig = JsonConvert.DeserializeObject<KafkaConfig>(json);
                // logger.LogInformation(kafkaConfig.KafkaService);
                // logger.LogInformation("kafkaConfig111");
            }
            logger.LogInformation("Kafka configuration file read succesfully");
            // }

            /** =============== Workload config file ================= */
            WorkloadConfig workloadConfig = null;
            if (workflowConfig.transactionSubmission)
            {
                logger.LogInformation("Init reading scenario configuration file...");
                using (StreamReader r = new StreamReader("workload_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("scenario_config.json contents:\n {0}", json);
                    workloadConfig = JsonConvert.DeserializeObject<WorkloadConfig>(json);
                }
                logger.LogInformation("Scenario file read succesfully");

                var list = workloadConfig.transactionDistribution.ToList();
                int lastPerc = 0;
                foreach(var entry in list)
                {
                    if (entry.Value < lastPerc) throw new Exception("Transaction distribution is incorrectly configured.");
                    lastPerc = entry.Value;
                }
                if(list.Last().Value != 100) throw new Exception("Transaction distribution is incorrectly configured.");

                /**
                 * The relation between prescribed concurrency level and minimum number of customers
                 * varies according to the distribution of transactions. The code below is a (very)
                 * safe threshold.
                 */
                if (dataLoadConfig.numCustomers < workloadConfig.concurrencyLevel)
                {
                    throw new Exception("Total number of customers must be higher than concurrency level");
                }

                if (workloadConfig.customerWorkerConfig.productUrl is null)
                {
                    throw new Exception("No products URL found! Execution suspended.");
                }
                if (workloadConfig.customerWorkerConfig.cartUrl is null)
                {
                    throw new Exception("No carts URL found! Execution suspended.");
                }
                if (workloadConfig.sellerWorkerConfig.productUrl is null)
                {
                    throw new Exception("No product URL found for seller! Execution suspended.");
                }
                if (workloadConfig.sellerWorkerConfig.sellerUrl is null)
                {
                    throw new Exception("No sellers URL found! Execution suspended.");
                }
                if (workloadConfig.deliveryWorkerConfig.shipmentUrl is null)
                {
                    throw new Exception("No shipment URL found! Execution suspended.");
                }

            }

            /** =============== Collection of metrics config file ================= */
            CollectionConfig collectionConfig = null;
            if (workflowConfig.collection)
            {
                logger.LogInformation("Init reading collection of metrics configuration file...");
                using (StreamReader r = new StreamReader("collection_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("collection_config.json contents:\n {0}", json);
                    collectionConfig = JsonConvert.DeserializeObject<CollectionConfig>(json);
                }
                logger.LogInformation("Collection of metrics file read succesfully");
            }

            MasterConfig masterConfiguration = new MasterConfig(            
                workflowConfig,dataLoadConfig, ingestionConfig, workloadConfig, kafkaConfig, collectionConfig
                // olistConfig = new DataGeneration.Real.OlistDataSourceConfiguration()
            );
            return masterConfiguration;
        }

    }

}