namespace NServiceBus
{
    using System;
    using Settings;
    using Transport;
    using TransportInfrastructure = TransportInfrastructure;

    public class GooglePubSubTransport : TransportDefinition
    {
        public override Transport.TransportInfrastructure Initialize(SettingsHolder settings, string connectionString)
        {
            if (!string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException($"{nameof(GooglePubSubTransport)} does not require a connection string, but a connection string was provided. Use the code based configuration methods instead.");
            }
            
            return new TransportInfrastructure(settings);
        }

        public override string ExampleConnectionStringForErrorMessage => "";

        public override bool RequiresConnectionString => false;
    }
}