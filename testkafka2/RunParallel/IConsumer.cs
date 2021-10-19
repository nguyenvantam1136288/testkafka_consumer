using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace testkafka2.RunParallel
{
    public class IConsumer
    {
        IPEndPoint ipEnd;
        Socket sock;
        private IConsumer<string, string> Consumer { get; set; }
        private IConsumer<string, byte[]> ConsumerFile { get; set; }
        public long MinTime { get; set; } = long.MaxValue;
        public long MaxTime { get; set; } = long.MinValue;
        public static int MessageCount = 0;
        public Guid Id { get; set; }

        private CancellationTokenSource cts = new CancellationTokenSource();
        public IConsumer(ConsumerConfig consumerConfig, CancellationTokenSource _cts)
        {
            Id = Guid.NewGuid();
            cts = _cts;
            Consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

            ConsumerFile = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();


            

        }
        public void Recei(string topic)//, Message<string, string> message)
        {
            Consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    var timerRecord = new Stopwatch();
                    timerRecord.Start();

                    var cr = Consumer.Consume(cts.Token);

                    strMessage(cr.Message.Key, cr.Message.Value);

                    //Console.WriteLine($"Consumed record with key {key} and value {cr.Message.Value}");
                  
                    Consumer.Commit(cr);

                    Interlocked.Increment(ref MessageCount);
                    timerRecord.Stop();
                    var t = timerRecord.ElapsedMilliseconds;
                    if (t < MinTime) MinTime = t;
                    if (t > MaxTime) MaxTime = t;

                    Console.WriteLine("Message Count: " + MessageCount + "  Consumer :" + Id.ToString() + " Thoi gian nhan 1 message  min: " + MinTime + " max: " + MaxTime + "  current: " + t);

                }
            }
            catch (OperationCanceledException)
            {
                //exception might have occurred since Ctrl-C was pressed.
            }
            finally
            {
                Consumer.Close();
            }
        }
        static string projectDirectory = Directory.GetParent(Environment.CurrentDirectory).Parent.FullName;
        public void ReceiFile(string topic)
        {
            ConsumerFile.Subscribe(topic);
            try
            {
                while (true)
                {
                    var timerRecord = new Stopwatch();
                    timerRecord.Start();

                    var cr = ConsumerFile.Consume(cts.Token);
                  
                    if(cr.Message.Value != null)
                    {
                        string mgsKey = cr.Message.Key;
                        byte[] mgsValue = cr.Message.Value;

                        string pathFull = projectDirectory + @"/ZipDirectory/Filezip/" + mgsKey;

                        File.WriteAllBytes(pathFull, mgsValue);

                        ////Giải nén file zip -> json
                        string output = projectDirectory + @"\ZipDirectory\Output";
                        try
                        {
                            ZipFile.ExtractToDirectory(pathFull, output);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Ten file da ton tai");
                            Console.ReadKey();
                        }
                    }

                    ConsumerFile.Commit(cr);


                }
            }
            catch (OperationCanceledException)
            {
                //exception might have occurred since Ctrl-C was pressed.
            }
            finally
            {
                ConsumerFile.Close();
            }
        }

        public void Close()
        {
            if (Consumer != null)
                Consumer.Dispose();
        }

        string strMessage(string key, string data)
        {
            string giaima = string.Empty;
            try
            {
                Console.WriteLine($"Lenght data: " + data.Length);
                #region Test convert data to obejct
                var Unicode = System.Text.ASCIIEncoding.Unicode.GetByteCount(data);
                var ASCII = System.Text.ASCIIEncoding.ASCII.GetByteCount(data);

                var Unicode_ToSize = GetSize.ToSize(Unicode, GetSize.SizeUnits.MB);
                var ASCII_ToSize = GetSize.ToSize(ASCII, GetSize.SizeUnits.MB);

                giaima = EncryptAndDecrypt.Decrypt(data, EncryptAndDecrypt.Password);

                var model = JsonConvert.DeserializeObject<List<User>>(giaima);
                Console.WriteLine($"Model count: " + model.Count() + " Unicode/ToSize: " + Unicode + "/"+ Unicode_ToSize
                    + " ASCII/ToSize: " + ASCII + "/" + ASCII_ToSize);
               
                //JObject rss = JObject.Parse(data);
                //User album = rss.ToObject<User>();
                #endregion
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error convert json to object");
                Console.ReadKey();
            }
            return giaima;
        }

       
    }
    public class User
    {
        public string Name { get; set; }

        public string Address { get; set; }

        public string Email { get; set; }
        public string PhoneNumber { get; set; }
        public string BookId { get; set; }
    }
}
