namespace NServiceBus.AcceptanceTests
{
    using System;
    using System.Threading.Tasks;
    using AcceptanceTesting.Support;

    public class ConfigureEndpointGooglePubSubTransport : IConfigureEndpointTestExecution
    {
        public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
        {
            var transportConfig = configuration.UseTransport<GooglePubSubTransport>();
            var projectId = Environment.GetEnvironmentVariable("GooglePubSubTransport.Project");

            if (string.IsNullOrEmpty(projectId))
            {
                throw new Exception("The 'GooglePubSubTransport.Project' environment variable is not set.");
            }

            transportConfig.Project(projectId);
            return Task.FromResult(0);
        }

        public Task Cleanup()
        {
            return Task.FromResult(0);
        }
    }
}