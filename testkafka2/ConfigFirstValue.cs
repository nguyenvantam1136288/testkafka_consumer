using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace testkafka2
{
    public class ConfigFirstValue
    {
        public static string ServerKafka = "34.223.53.19:9092";
        public static ConsumerConfig ConsumerConfig()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = ServerKafka,
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var consumerConfig = new ConsumerConfig(config);
            consumerConfig.GroupId = "dotnet-oss-consumer-group";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.EnableAutoCommit = false;
            return consumerConfig;
        }


        public static string Topic = "testTopic";//testTopic topic10Part topic20Part
        public static int ConsumerNumber = 1;
    }
}
