using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.IO.Packaging;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using testkafka2.RunParallel;
using static System.Net.WebRequestMethods;

namespace testkafka2
{
    class Program
    {
        static void Main(string[] args)
        {
            string startPath = @"D:\testZipfile\inputdir";
            string zipPath = @"D:\testZipfile\payawe.zip";

            string extractPath = @"D:\testZipfile";

            //ZipFile.CreateFromDirectory(startPath, zipPath);

            //ZipFile.ExtractToDirectory(zipPath, extractPath);
            //Console.ReadLine();

            //            string pathFull;
            //            //Giải nén
            //            string extractPath = @"D:\A.Diep";
            ////\example\testkafka_consumer\testkafka2\ZipDirectory\Output";
            //            //
            //            //D:\A.Diep\example\testkafka_consumer\testkafka2\ZipDirectory\Filezip
            //            pathFull = @"D:/A.Diep/example/testkafka_consumer/testkafka2/ZipDirectory/Filezip/Payawe_18102021_NewGuid.zip";
            //            pathFull = @"D:\Payawe_18102021_NewGuid2.zip";
            //            ZipFile.ExtractToDirectory(pathFull, extractPath);

            Receiver.CreateTConsumers(ConfigFirstValue.ConsumerNumber, ConfigFirstValue.ConsumerConfig());
            //Receiver.ReciverData();
            Receiver.ReciverDataFile();
            //Consume(topic, config);

            Console.ReadLine();
        }


        static void Consume(string topic, ClientConfig config)
        {
            int i = 0;
            var consumerConfig = new ConsumerConfig(config);
            consumerConfig.GroupId = "dotnet-oss-consumer-group";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.EnableAutoCommit = false;//true;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };
          
            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topic);
                try
                {
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        string key = cr.Message.Key == null ? "Null" : cr.Message.Key;
                        i++;
                        Console.WriteLine($"\n" + i);
                        //Console.WriteLine($"Consumed record with key {key} and value {cr.Message.Value}");

                        //consumer.Commit();//nhận hoàn thành rồi báo để gửi cho thằng khác
                    }
                }
                catch (OperationCanceledException)
                {
                    //exception might have occurred since Ctrl-C was pressed.
                }
                finally
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }
    }
}
