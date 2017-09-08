namespace NServiceBus
{
    using Configuration.AdvancedExtensibility;

    public static class GooglePubSubTransportExtensions
    {
        public static TransportExtensions<GooglePubSubTransport> Project(this TransportExtensions<GooglePubSubTransport> transportExtensions, string projectId)
        {
            transportExtensions.GetSettings().Set(SettingsKeys.ProjectId, projectId);
            return transportExtensions;
        }
    }
}