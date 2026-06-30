using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.Conformance.Tests;

/// <summary>
/// Runs all conformance tests against the in-process emulator.
/// </summary>
public class EmulatorConformanceTests : ConformanceTestBase
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    protected override async Task<(ServiceBusClient? client, ServiceBusAdministrationClient? admin)> CreateClientsAsync()
    {
        await _fixture.StartAsync();

        // Emulator is plaintext; the fixture's ConnectionString carries
        // `UseDevelopmentEmulator=true` so the Azure SDK uses plain AMQP/HTTP.
        var connectionString = _fixture.ConnectionString;
        ConnectionString = connectionString;

        var client = new ServiceBusClient(connectionString, new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions
            {
                MaxRetries = 0,
                TryTimeout = TimeSpan.FromSeconds(10)
            }
        });

        var admin = new ServiceBusAdministrationClient(connectionString);

        return (client, admin);
    }

    public new async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _fixture.DisposeAsync();
    }
}
