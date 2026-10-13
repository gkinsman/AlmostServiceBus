using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using AlmostServiceBus.Aspire.Hosting;

namespace AlmostServiceBus.Aspire.Tests;

public class ConnectionStringTests
{
    [Fact]
    public void Connection_string_takes_host_and_port_from_the_servicebus_endpoint()
    {
        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder.AddServiceBusEmulator("servicebus").Resource;

        var expression = resource.ConnectionStringExpression;

        Assert.Equal(
            "Endpoint=sb://{0};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true",
            expression.Format);
        var provider = Assert.IsType<EndpointReferenceExpression>(Assert.Single(expression.ValueProviders));
        Assert.Equal(EndpointProperty.HostAndPort, provider.Property);
        Assert.Equal("servicebus", provider.Endpoint.EndpointName);
    }

    [Fact]
    public async Task Host_processes_get_localhost()
    {
        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder.AddServiceBusEmulator("servicebus", port: 5780).Resource;
        var endpoint = ServiceBusEndpoint(resource);
        endpoint.AllocatedEndpoint = new AllocatedEndpoint(endpoint, "localhost", 5780);

        var value = await resource.ConnectionStringExpression.GetValueAsync(CancellationToken.None);

        Assert.StartsWith("Endpoint=sb://localhost:5780;", value);
    }

    [Fact]
    public async Task Containers_get_the_host_allocated_on_the_container_network()
    {
        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder.AddServiceBusEmulator("servicebus", port: 5780).Resource;
        var endpoint = ServiceBusEndpoint(resource);
        endpoint.AllocatedEndpoint = new AllocatedEndpoint(endpoint, "localhost", 5780);
        var network = KnownNetworkIdentifiers.DefaultAspireContainerNetwork;
        endpoint.AllAllocatedEndpoints.AddOrUpdateAllocatedEndpoint(
            network,
            new AllocatedEndpoint(endpoint, KnownHostNames.DefaultContainerTunnelHostName, 5780, EndpointBindingMode.SingleAddress, null, network));

        var value = await resource.ConnectionStringExpression.GetValueAsync(
            new ValueProviderContext { Network = network },
            CancellationToken.None);

        Assert.StartsWith($"Endpoint=sb://{KnownHostNames.DefaultContainerTunnelHostName}:5780;", value);
    }

    // Aspire's container tunnel connects to the emulator over loopback, so a referencing
    // container must not widen the bind address.
    [Fact]
    public async Task Referencing_container_does_not_change_the_bind_host()
    {
        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var emulator = builder.AddServiceBusEmulator("servicebus");
        builder.AddContainer("consumer", "alpine").WithReference(emulator);

        var env = await ResolveEnvironmentAsync(emulator.Resource);

        Assert.DoesNotContain("ASB_BIND_HOST", env.Keys);
    }

    private static EndpointAnnotation ServiceBusEndpoint(IResource resource) =>
        resource.Annotations.OfType<EndpointAnnotation>().Single(e => e.Name == "servicebus");

    private static async Task<Dictionary<string, object>> ResolveEnvironmentAsync(IResource resource)
    {
        var env = new Dictionary<string, object>();
        var context = new EnvironmentCallbackContext(new DistributedApplicationExecutionContext(DistributedApplicationOperation.Run), env);
        foreach (var annotation in resource.Annotations.OfType<EnvironmentCallbackAnnotation>())
            await annotation.Callback(context);
        return env;
    }
}
