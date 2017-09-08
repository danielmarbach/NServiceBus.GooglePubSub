using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Routing;
using NServiceBus.Settings;
using NServiceBus.Transport;

class TransportInfrastructure : NServiceBus.Transport.TransportInfrastructure
{
    public TransportInfrastructure(SettingsHolder settings)
    {
        if (!settings.TryGet(SettingsKeys.ProjectId, out projectId))
        {
            throw new InvalidOperationException("Set project id");
        }
    }

    public override IEnumerable<Type> DeliveryConstraints
    {
        get
        {
            var constraints = new List<Type>();

            return constraints;
        }
    }

    public override TransportTransactionMode TransactionMode => TransportTransactionMode.ReceiveOnly;
    public override OutboundRoutingPolicy OutboundRoutingPolicy => new OutboundRoutingPolicy(OutboundRoutingType.Unicast, OutboundRoutingType.Multicast, OutboundRoutingType.Unicast);

    public override TransportReceiveInfrastructure ConfigureReceiveInfrastructure()
    {
        return new TransportReceiveInfrastructure(() => new MessagePump(projectId), () => new QueueCreator(projectId), () => Task.FromResult(StartupCheckResult.Success));
    }

    public override TransportSendInfrastructure ConfigureSendInfrastructure()
    {
        return new TransportSendInfrastructure(() => new Dispatcher(projectId), () => Task.FromResult(StartupCheckResult.Success));
    }

    public override TransportSubscriptionInfrastructure ConfigureSubscriptionInfrastructure()
    {
        return new TransportSubscriptionInfrastructure(() => new SubscriptionManager(projectId));
    }

    public override EndpointInstance BindToLocalEndpoint(EndpointInstance instance)
    {
        return instance;
    }

    public override string ToTransportAddress(LogicalAddress logicalAddress)
    {
        var queue = new StringBuilder(logicalAddress.EndpointInstance.Endpoint);

        if (logicalAddress.EndpointInstance.Discriminator != null)
        {
            queue.Append("-" + logicalAddress.EndpointInstance.Discriminator);
        }

        if (logicalAddress.Qualifier != null)
        {
            queue.Append("." + logicalAddress.Qualifier);
        }

        return queue.ToString();
    }

    string projectId;
}