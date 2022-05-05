using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace DLQ
{
    public class QueueClearer
    {
        private string connectionString;
        private ServiceBusClient client;
        private ServiceBusAdministrationClient adminClient;

        public QueueClearer(string connectionString)
        {
            this.connectionString = connectionString;
            this.client = new ServiceBusClient(connectionString);
            this.adminClient = new ServiceBusAdministrationClient(connectionString);
        }

        public async Task Clear(string queueName)
        {
            // receive the dead lettered message with receiver scoped to the dead letter queue.
            ServiceBusReceiver dlqReceiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter
            });

            int batchSize = 50;
            QueueRuntimeProperties p = await this.adminClient.GetQueueRuntimePropertiesAsync(queueName);

            while (p.DeadLetterMessageCount > 0)
            {
                Console.Write($"Clearing {p.DeadLetterMessageCount} DLQ messages in batch sizes of {batchSize}...");
                IReadOnlyList<ServiceBusReceivedMessage> dlqMessages = await dlqReceiver.ReceiveMessagesAsync(batchSize);
                foreach (var m in dlqMessages)
                {
                    await dlqReceiver.CompleteMessageAsync(m);
                }
                Console.WriteLine("DONE.");

                // NOTE, sometimes the count seems off.. might need a wait?
                await Task.Delay(3);
                p = await this.adminClient.GetQueueRuntimePropertiesAsync(queueName);
            }            
        }
    }
}
