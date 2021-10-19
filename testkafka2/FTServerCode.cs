using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace testkafka2
{
    public class FTServerCode
    {
        public static void StartServer(byte[] clientData)
        {
            try
            {
                int fileNameLen = BitConverter.ToInt32(clientData, 0);
                string fileName = Encoding.ASCII.GetString(clientData, 4, fileNameLen);
                try
                {
                    string path = "D://A.Diep//example//testkafka_consumer//testkafka2//ZipDirectory//Filezip";
                    string pathFull = Path.Combine(path, fileName);

                    File.WriteAllBytes(pathFull, clientData);

                    //using (var fs = new FileStream(pathFull, FileMode.Create, FileAccess.Write))
                    //{
                    //    fs.Write(clientData, 0, clientData.Length);
                    //}

                    ////Giải nén
                    //string extractPath = @"D:/A.Diep/example/testkafka_consumer/testkafka2/ZipDirectory/Output";
                    ////
                    ////D:\A.Diep\example\testkafka_consumer\testkafka2\ZipDirectory\Filezip
                    //pathFull = @"D:/A.Diep/example/testkafka_consumer/testkafka2/ZipDirectory/Filezip/payawe.zip";
                    ////pathFull = @"D:\Payawe_18102021_NewGuid2.zip";
                    //ZipFile.ExtractToDirectory(pathFull, extractPath);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception caught in process: {0}", ex);
                }
            }
            catch (Exception ex)
            {
                
            }
        }
    }
}
