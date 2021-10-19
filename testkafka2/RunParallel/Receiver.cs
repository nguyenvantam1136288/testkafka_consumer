using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace testkafka2.RunParallel
{
    public class Receiver
    {
        private static readonly ConcurrentDictionary<int, IConsumer> IConsumers = new ConcurrentDictionary<int, IConsumer>();
        private static int ConsumerCount { get; set; }
        private static readonly ConcurrentDictionary<Guid, Task> Tasks = new ConcurrentDictionary<Guid, Task>();
        
        public static void CreateTConsumers(int number, ConsumerConfig consumerConfig)
        {
            ConsumerCount = number;
            for (int i = 0; i < number; i++)
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; 
                    cts.Cancel();
                };
                IConsumers[i] = new IConsumer(consumerConfig, cts);
            }
        }
        public static void ReciverData()
        {
            for (int i = 0; i < ConsumerCount; i++)
            {
                int ii = i;
                Tasks[Guid.NewGuid()] = Task.Run(() =>
                {
                    int iii = ii;
                    var consumer = IConsumers[iii];
                    consumer.Recei(ConfigFirstValue.Topic);
                });
            }
            //Console.WriteLine("Producer: " + ProducerCount + " messageCount moi pro: " + messageCount);
        }

        public static void ReciverDataFile()
        {
            for (int i = 0; i < ConsumerCount; i++)
            {
                int ii = i;
                Tasks[Guid.NewGuid()] = Task.Run(() =>
                {
                    int iii = ii;
                    var consumer = IConsumers[iii];
                    consumer.ReceiFile(ConfigFirstValue.Topic);
                });
            }
        }
        public static void Close()
        {
            Tasks.Clear();
        }
    }
}
