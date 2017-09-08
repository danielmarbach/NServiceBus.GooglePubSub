using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using NServiceBus.Extensibility;
using NServiceBus.Transport;

class Dispatcher : IDispatchMessages
{
    string projectId;

    public Dispatcher(string projectId)
    {
        this.projectId = projectId;
    }

    public async Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, ContextBag context)
    {
        var publishers = new ConcurrentStack<SimplePublisher>();
        try
        {
            var unicastTransportOperations = outgoingMessages.UnicastTransportOperations;
            var multicastTransportOperations = outgoingMessages.MulticastTransportOperations;
            
            var tasks = new List<Task>(unicastTransportOperations.Count + multicastTransportOperations.Count);

            foreach (var operation in unicastTransportOperations)
            {
                tasks.Add(SendMessage(operation, publishers));
            }

            foreach (var operation in multicastTransportOperations)
            {
                tasks.Add(SendMessage(operation, publishers));
            }

            await (tasks.Count == 1 ? tasks[0] : Task.WhenAll(tasks))
                .ConfigureAwait(false);
        }
        finally
        {
            var stopTasks = new List<Task>(publishers.Count);
            foreach (var publisher in publishers.ToArray())
            {
                stopTasks.Add(publisher.ShutdownAsync(CancellationToken.None));
            }
            await (stopTasks.Count == 1 ? stopTasks[0] : Task.WhenAll(stopTasks))
                .ConfigureAwait(false);
        }
    }

    async Task SendMessage(UnicastTransportOperation transportOperation, ConcurrentStack<SimplePublisher> publishers)
    {
        var simplePublisher = await SimplePublisher.CreateAsync(new TopicName(projectId, transportOperation.Destination));
        var message = transportOperation.Message;
        var transportMessage = new PubsubMessage
        {
            MessageId = message.MessageId,
            Data = ByteString.CopyFrom(message.Body)
        };
        transportMessage.Attributes.Add(message.Headers);
        await simplePublisher.PublishAsync(transportMessage)
            .ConfigureAwait(false);
        publishers.Push(simplePublisher);
    }

    async Task SendMessage(MulticastTransportOperation transportOperation, ConcurrentStack<SimplePublisher> publishers)
    {
        var generateRoutingKey = DefaultRoutingKeyConvention.GenerateRoutingKey(transportOperation.MessageType);
        var simplePublisher = await SimplePublisher.CreateAsync(new TopicName(projectId, generateRoutingKey));
        var message = transportOperation.Message;
        var transportMessage = new PubsubMessage
        {
            MessageId = message.MessageId,
            Data = ByteString.CopyFrom(message.Body)
        };
        transportMessage.Attributes.Add(message.Headers);
        await simplePublisher.PublishAsync(transportMessage)
            .ConfigureAwait(false);
        publishers.Push(simplePublisher);
    }
}