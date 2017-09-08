using System;
using System.Threading.Tasks;
using NServiceBus.Extensibility;
using NServiceBus.Transport;

class SubscriptionManager : IManageSubscriptions
{
    string projectId;

    public SubscriptionManager(string projectId)
    {
        this.projectId = projectId;
    }

    public async Task Subscribe(Type eventType, ContextBag context)
    {
        
    }

    public async Task Unsubscribe(Type eventType, ContextBag context)
    {
    }
}