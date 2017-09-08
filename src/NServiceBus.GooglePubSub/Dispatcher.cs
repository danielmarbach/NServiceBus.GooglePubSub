using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using NServiceBus.Extensibility;
using NServiceBus.Transport;

class Dispatcher : IDispatchMessages
{
    public Dispatcher(string projectId)
    {
        this.projectId = projectId;
    }

    public async Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, ContextBag context)
    {
        var unicastTransportOperations = outgoingMessages.UnicastTransportOperations;
        var multicastTransportOperations = outgoingMessages.MulticastTransportOperations;

        var tasks = new List<Task>(unicastTransportOperations.Count + multicastTransportOperations.Count);

        foreach (var operation in unicastTransportOperations)
        {
            tasks.Add(SendMessage(operation));
        }

        foreach (var operation in multicastTransportOperations)
        {
            tasks.Add(SendMessage(operation));
        }

        await (tasks.Count == 1 ? tasks[0] : Task.WhenAll(tasks))
            .ConfigureAwait(false);
    }

    async Task SendMessage(UnicastTransportOperation transportOperation)
    {
        var publisher = await PublisherClient.CreateAsync();
        var message = transportOperation.Message;
        var transportMessage = new PubsubMessage
        {
            MessageId = message.MessageId,
            Data = ByteString.CopyFrom(message.Body)
        };
        transportMessage.Attributes.Add(message.Headers);
        await publisher.PublishAsync(new TopicName(projectId, transportOperation.Destination), new[]
            {
                transportMessage
            })
            .ConfigureAwait(false);
    }

    async Task SendMessage(MulticastTransportOperation transportOperation)
    {
        var generateRoutingKey = DefaultRoutingKeyConvention.GenerateRoutingKey(transportOperation.MessageType);
        var publisher = await PublisherClient.CreateAsync();
        var message = transportOperation.Message;
        var transportMessage = new PubsubMessage
        {
            MessageId = message.MessageId,
            Data = ByteString.CopyFrom(message.Body)
        };
        transportMessage.Attributes.Add(message.Headers);
        await publisher.PublishAsync(new TopicName(projectId, generateRoutingKey), new[]
            {
                transportMessage
            })
            .ConfigureAwait(false);
    }

    string projectId;
}