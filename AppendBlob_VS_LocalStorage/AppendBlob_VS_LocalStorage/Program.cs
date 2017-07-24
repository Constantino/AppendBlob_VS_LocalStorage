using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Diagnostics;
using System.IO;

namespace AppendBlob_VS_LocalStorage
{
    class Program
    {

        static void Main(string[] args)
        {



            string keyValue = ConfigurationManager.AppSettings["BlobKey"];
            string storageAccountName = ConfigurationManager.AppSettings["storageAccountName"];
            string containerName  = ConfigurationManager.AppSettings["containerName"];
            
            var credentials = new StorageCredentials(storageAccountName, keyValue);
            var storageAccount = new CloudStorageAccount(credentials, storageAccountName, null, true);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);

            int times = 30734;

            //RunLocalStorage(container, times);
            //RunMemoryHolder(container, times);
            RunAppendBlob(container, times);
        }

        public static void RunMemoryHolder(CloudBlobContainer container, int times) {
            
            CloudBlockBlob blockBlob = container.GetBlockBlobReference("blockBlob/test.csv");

            int nLabels = 10;

            string record = FillRecord("thing", nLabels);

            var watcher = Stopwatch.StartNew();
            
            for (int i = 0; i < times; i++)
            {
                Console.WriteLine(((i + 1.0) / times) * 100 + "%");

                record += record;
            }
            
            blockBlob.UploadText(record);

            Console.WriteLine("ElapsedTime: " + watcher.Elapsed);
            Console.WriteLine("Blob length (bytes): " + blockBlob.Properties.Length);

            Console.Read();
        }

        public static void RunLocalStorage(CloudBlobContainer container, int times) {
            string tempPath = Path.GetTempPath();
            string fileName = string.Format("testLocalStorage.csv");
            string sourceFile = Path.Combine(tempPath, fileName);
            StreamWriter file = new StreamWriter(sourceFile);

            CloudBlockBlob blockBlob = container.GetBlockBlobReference("blockBlob/test.csv");
            
            int nLabels = 10;

            string record = FillRecord("thing", nLabels);

            var watcher = Stopwatch.StartNew();

            
            file.Write(record);
            file.Flush();

            for (int i = 0; i < times; i++)
            {
                Console.WriteLine(((i + 1.0) / times) * 100 + "%");
                
                file.Write(record);
                file.Flush();

            }

            file.Close();

            blockBlob.UploadFromFile(sourceFile);

            Console.WriteLine("ElapsedTime: " + watcher.Elapsed);
            Console.WriteLine("Blob length (bytes): " + blockBlob.Properties.Length);

            Console.Read();
            
        }

        public static void RunAppendBlob(CloudBlobContainer container, int times) {
            CloudAppendBlob appendBlob = container.GetAppendBlobReference("x/test.csv");
            appendBlob.CreateOrReplace();
            
            int nLabels = 10;

            string record = FillRecord("thing", nLabels);

            var watcher = Stopwatch.StartNew();

            appendBlob.AppendText(record);

            for (int i = 0; i < times; i++)
            {
                Console.WriteLine(((i + 1.0) / times) * 100 + "%");
                appendBlob.AppendText(record);

            }

            Console.WriteLine("ElapsedTime: " + watcher.Elapsed);
            Console.WriteLine("Blob length (bytes): " + appendBlob.Properties.Length);

            Console.Read();
        }



        public static string FillRecord(string dummyText, int nLabels) {

            string record = string.Empty;

            for (int i = 0; i < nLabels; i++)
            {
                record += dummyText + i;

                if (i != nLabels - 1)
                {
                    record += ",";
                }

            }

            return record+"\n";
        }
    }
}
