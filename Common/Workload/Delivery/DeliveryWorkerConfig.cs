namespace Common.Workload.Delivery
{
	public class DeliveryWorkerConfig
	{
        public string shipmentUrl { get; set; }

        public TargetPlatform targetPlatform { get; set; }

        public int proxyAddress { get; set; }
    }
}

