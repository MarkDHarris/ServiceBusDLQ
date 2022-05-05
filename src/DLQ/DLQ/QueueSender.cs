using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLQ
{
    // FROM https://www.nuget.org/packages/Azure.Messaging.ServiceBus/7.8.0-beta.2

    public class QueueSender
    {
        private string connectionString;
        private ServiceBusClient client;
        private ServiceBusAdministrationClient adminClient;

        public QueueSender(string connectionString)
        {
            this.connectionString = connectionString;
            this.client = new ServiceBusClient(connectionString);
            this.adminClient = new ServiceBusAdministrationClient(connectionString);
        }

        public async Task Send(string queueName, string message)
        {
            ServiceBusSender sender = client.CreateSender(queueName);
            ServiceBusMessage msg = new ServiceBusMessage(message);
            await sender.SendMessageAsync(msg);
        }

        public async Task SendBatch(string queueName, List<string> messages)
        {
            ServiceBusSender sender = client.CreateSender(queueName);

            Queue<ServiceBusMessage> msgs = new Queue<ServiceBusMessage>();
            foreach(var m in messages)
            {
                msgs.Enqueue(new ServiceBusMessage(m));
            }

            int messageCount = msgs.Count;

            while (msgs.Count > 0)
            {
                // start a new batch
                using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

                // add the first message to the batch
                if (messageBatch.TryAddMessage(msgs.Peek()))
                {
                    // dequeue the message from the .NET queue once the message is added to the batch
                    msgs.Dequeue();
                }
                else
                {
                    // if the first message can't fit, then it is too large for the batch
                    throw new Exception($"Message {messageCount - msgs.Count} is too large and cannot be sent.");
                }

                // add as many messages as possible to the current batch
                while (msgs.Count > 0 && messageBatch.TryAddMessage(msgs.Peek()))
                {
                    // dequeue the message from the .NET queue as it has been added to the batch
                    msgs.Dequeue();
                }

                // now, send the batch
                await sender.SendMessagesAsync(messageBatch);
                // if there are any remaining messages in the .NET queue, the while loop repeats
            }
        }

        public async Task DeadLetterAllTestMessages(string queueName)
        {
            QueueRuntimeProperties p = await this.adminClient.GetQueueRuntimePropertiesAsync(queueName);
            long activeCount = p.ActiveMessageCount;

            
            ServiceBusReceiver receiver = client.CreateReceiver(queueName);

            for(int i=0; i<activeCount; i++)
            {
                ServiceBusReceivedMessage receivedMessage = await receiver.ReceiveMessageAsync();
                var content = receivedMessage.Body.ToString();

                // dead-letter the message, thereby preventing the message from being received again without receiving from the dead letter queue.
                await receiver.DeadLetterMessageAsync(receivedMessage);
            }            
        }

        public async Task<long> GetActiveMessageCount(string queueName)
        {
            QueueRuntimeProperties p = await this.adminClient.GetQueueRuntimePropertiesAsync(queueName);
            return p.ActiveMessageCount;
        }
    }
}
