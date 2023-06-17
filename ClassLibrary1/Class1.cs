using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Renci.SshNet;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyFunctionApp
{
    public static class ServiceBusTriggerFunction
    {
        private const int BatchSize = 100; // Adjust batch size as needed

        [Function("ServiceBusTriggerFunction")]
        public static async Task Run(
            [ServiceBusTrigger("mytopic", "mysubscription")] ServiceBusReceivedMessage[] messages,
            FunctionContext context)
        {
            var logger = context.GetLogger("ServiceBusTriggerFunction");

            logger.LogInformation($"Processing {messages.Length} messages.");

            var messageBatches = SplitIntoBatches(messages, BatchSize);
            var tasks = new List<Task>();

            foreach (var batch in messageBatches)
            {
                tasks.Add(ProcessBatchAsync(batch, logger));
            }

            await Task.WhenAll(tasks);

            logger.LogInformation("All messages processed.");
        }

        private static async Task ProcessBatchAsync(ServiceBusReceivedMessage[] batch, ILogger logger)
        {
            var sftpConnectionInfo = new SftpConnectionInfo("sftp.example.com", "username", "password");

            using (var sftpClient = new SftpClient(sftpConnectionInfo))
            {
                sftpClient.Connect();

                var uploadTasks = new List<Task>();

                foreach (var message in batch)
                {
                    var messageBody = Encoding.UTF8.GetString(message.Body);
                    var fileName = $"{message.MessageId}.txt";
                    var filePath = Path.Combine(Path.GetTempPath(), fileName);

                    await File.WriteAllTextAsync(filePath, messageBody);

                    uploadTasks.Add(UploadFileAsync(sftpClient, filePath, fileName, logger));
                }

                await Task.WhenAll(uploadTasks);

                sftpClient.Disconnect();
            }
        }

        private static async Task UploadFileAsync(SftpClient sftpClient, string filePath, string fileName, ILogger logger)
        {
            using (var fileStream = File.OpenRead(filePath))
            {
                await sftpClient.UploadFileAsync(fileStream, $"/upload/{fileName}");
            }

            logger.LogInformation($"File {fileName} uploaded to SFTP.");
        }

        private static IEnumerable<ServiceBusReceivedMessage[]> SplitIntoBatches(ServiceBusReceivedMessage[] messages, int batchSize)
        {
            return messages
                .Select((message, index) => new { Index = index, Message = message })
                .GroupBy(x => x.Index / batchSize)
                .Select(group => group.Select(x => x.Message).ToArray());
        }
    }
}