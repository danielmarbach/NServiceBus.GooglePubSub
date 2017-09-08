using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using NServiceBus.Transport;

class QueueCreator : ICreateQueues
{
    public QueueCreator(string projectId)
    {
        this.projectId = projectId;
    }

    public async Task CreateQueueIfNecessary(QueueBindings queueBindings, string identity)
    {
        var client = await PublisherClient.CreateAsync()
            .ConfigureAwait(false);

        var addresses = queueBindings.ReceivingAddresses.Concat(queueBindings.SendingAddresses).ToArray();
        var tasks = new List<Task>(addresses.Length);
        foreach (var address in addresses)
        {
            tasks.Add(CreateTopic(client, projectId, address));
        }
        await Task.WhenAll(tasks)
            .ConfigureAwait(false);
    }

    static async Task CreateTopic(PublisherClient publisherClient, string projectId, string topicName)
    {
        try
        {
            await publisherClient.CreateTopicAsync(new TopicName(projectId, topicName))
                .ConfigureAwait(false);
        }
        catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
        {
        }
    }

    string projectId;
}