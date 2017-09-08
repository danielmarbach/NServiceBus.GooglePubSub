using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Transport;

class MessagePump : IPushMessages 
{
    Func<MessageContext, Task> onMessage;
    Func<ErrorContext, Task<ErrorHandleResult>> onError;
    CriticalError criticalError;
    string projectId;
    SubscriberClient subscriberClient;
    SubscriptionName subscriptionName;
    SimpleSubscriber subscriber;
    static TransportTransaction transportTransaction = new TransportTransaction();
    static byte[] emptyBody = new byte[0];
    static ContextBag contextBag = new ContextBag();

    public MessagePump(string projectId)
    {
        this.projectId = projectId;
    }

    public async Task Init(Func<MessageContext, Task> onMessage, Func<ErrorContext, Task<ErrorHandleResult>> onError, CriticalError criticalError, PushSettings settings)
    {
        this.criticalError = criticalError;
        this.onError = onError;
        this.onMessage = onMessage;

        try
        {
            subscriberClient = await SubscriberClient.CreateAsync()
                .ConfigureAwait(false);
            subscriptionName = new SubscriptionName(projectId, settings.InputQueue);
            await subscriberClient.CreateSubscriptionAsync(subscriptionName, new TopicName(projectId, settings.InputQueue), pushConfig: null, ackDeadlineSeconds: 60)
                .ConfigureAwait(false);
        }
        catch (RpcException e) when(e.Status.StatusCode == StatusCode.AlreadyExists)
        {
        }
    }

    public void Start(PushRuntimeSettings limitations)
    {
        SimpleSubscriber.CreateAsync(subscriptionName)
            .ContinueWith(async t =>
            {
                var s = await t.ConfigureAwait(false);
                await s.StartAsync(Consume)
                    .ConfigureAwait(false);
                subscriber = s;
            });
    }

    async Task<SimpleSubscriber.Reply> Consume(PubsubMessage message, CancellationToken token)
    {
        using (var tokenSource = new CancellationTokenSource())
        {
            var processed = false;
            var errorHandled = false;
            var numberOfDeliveryAttempts = 0;
            var headers = message.Attributes.ToDictionary(k => k.Key, v => v.Value);
            var body = message.Data.ToByteArray();
            var messageId = message.MessageId;

            while (!processed && !errorHandled)
            {
                try
                {
                    var messageContext = new MessageContext(messageId, headers, body ?? emptyBody, transportTransaction, tokenSource, contextBag);
                    await onMessage(messageContext).ConfigureAwait(false);
                    processed = true;
                }
                catch (Exception ex)
                {
                    ++numberOfDeliveryAttempts;
                    var errorContext = new ErrorContext(ex, headers, messageId, body ?? emptyBody, transportTransaction, numberOfDeliveryAttempts);
                    errorHandled = await onError(errorContext).ConfigureAwait(false) == ErrorHandleResult.Handled;
                }
            }

            if (processed && tokenSource.IsCancellationRequested)
            {
                return SimpleSubscriber.Reply.Nack;
            }
            return SimpleSubscriber.Reply.Ack;
        }
    }

    public async Task Stop()
    {
        if (subscriber != null)
        {
            await subscriber.StopAsync(CancellationToken.None)
                .ConfigureAwait(false);
        }
    }
}