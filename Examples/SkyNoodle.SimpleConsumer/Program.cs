using System;
using Newtonsoft.Json;
using SkyNoodle.Core.Consumers;
using SkyNoodle.Core.Models;
using SkyNoodle.Core.Processors;

namespace SkyNoodle.SimpleConsumer
{
    public class Program
    {
        static void Main(string[] args)
        {
            //var brokerList = args[0];
            //var consumerGroup = args[1];

            var brokerList = "kafkatempest01.sit.williamhill.internal:9092";
            var consumerGroup = "skynoodle.test";

            new Program().Run(brokerList, consumerGroup);
        }

        public void Run(string brokerList, string consumerGroup)
        {
            var consumer = new SkyNoodleConsumer(brokerList, consumerGroup)
                .WithTopics("tempest.bet.settled")
                .WithStreamProcessor("NotifyMarketSettle", new MarketSettleStreamProcessor())
                .WithStreamProcessor("NotifySingleBetSettle", s =>
                {
                    Console.WriteLine($"Received {s}");
                });


            while (true)
            {
                consumer.ReceiveMessage(TimeSpan.FromSeconds(1));
            }
        }
    }

    public class MarketSettleStreamProcessor : IStreamProcessor
    {
        public void ProcessMessage(string messagePayload)
        {
            var marketSettle = JsonConvert.DeserializeObject<MarketSettle>(messagePayload);
            Console.WriteLine($"{JsonConvert.SerializeObject(marketSettle)}");
        }
    }

    public class MarketSettle
    {
        public string MarketId { get; set; }
        public DateTime SettlementTimestamp { get; set; }
    }

    public class SingleBetSettle
    {
        public long InternalBetId { get; set; }
        public long ExternalBetId { get; set; }
        public DateTime SettlementTimestamp { get; set; }
    }

    //public class SingleBetSettleStreamProcessor : IStreamProcessor
    //{
    //    public void ProcessMessage(ScramJetMessage message)
    //    {
    //        var singleBetSettle = JsonConvert.DeserializeObject<SingleBetSettle>(message.Payload.ToString());
    //        Console.WriteLine($"{JsonConvert.SerializeObject(singleBetSettle)}");
    //    }
    //}

    //.WithStreamProcessor("NotifySingleBetSettle", message =>
    //{
    //var singleBetSettle = JsonConvert.DeserializeObject<SingleBetSettle>(message.Payload.ToString());
    //Console.WriteLine($"{JsonConvert.SerializeObject(singleBetSettle)}");
    //return true;
    //});
}
