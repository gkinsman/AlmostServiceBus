using System.Net;
using System.Net.Sockets;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;

namespace AlmostServiceBus.MassTransit.Tests
{

/// <summary>
/// Tests using the real Azure SDK ServiceBusAdministrationClient with entity names
/// that contain '/' — the pattern MassTransit uses (Namespace/TypeName).
///
/// This validates the emulator handles multi-segment entity paths correctly,
/// which is required for MassTransit compatibility.
/// </summary>
public class RealTopologyTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private ServiceBusAdministrationClient _adminClient = null!;

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();

        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (context, ct) =>
            {
                var port = context.DnsEndPoint.Port;
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(IPAddress.Loopback, port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
        };

        var options = new ServiceBusAdministrationClientOptions();
        options.Transport = new HttpClientTransport(new HttpClient(handler));

        _adminClient = new ServiceBusAdministrationClient(
            _fixture.ConnectionString,
            options);
    }

    public async Task DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    [Fact]
    public async Task CreateTopic_WithSlashInName_Succeeds()
    {
        // MassTransit generates topic names like "Namespace/EventType"
        var topicName = "AlmostServiceBus.MassTransit.Tests.Contracts/OrderPlaced";

        var response = await _adminClient.CreateTopicAsync(topicName);

        Assert.Equal(topicName, response.Value.Name);
    }

    [Fact]
    public async Task CreateQueue_WithSlashInName_Succeeds()
    {
        var queueName = "AlmostServiceBus.MassTransit.Tests/order-processor";

        var response = await _adminClient.CreateQueueAsync(queueName);

        Assert.Equal(queueName, response.Value.Name);
    }

    [Fact]
    public async Task GetTopic_WithSlashInName_Succeeds()
    {
        var topicName = "My.Long.Namespace/Events-SomethingHappened";
        await _adminClient.CreateTopicAsync(topicName);

        var response = await _adminClient.GetTopicAsync(topicName);

        Assert.Equal(topicName, response.Value.Name);
    }

    [Fact]
    public async Task CreateSubscription_OnTopicWithSlash_Succeeds()
    {
        var topicName = "My.App.Domain/Events-UserCreated";
        await _adminClient.CreateTopicAsync(topicName);
        await _adminClient.CreateQueueAsync("user-handler");

        var subOptions = new CreateSubscriptionOptions(topicName, "user-handler-sub")
        {
            ForwardTo = "user-handler"
        };
        var response = await _adminClient.CreateSubscriptionAsync(subOptions);

        Assert.Equal("user-handler-sub", response.Value.SubscriptionName);
        Assert.Equal(topicName, response.Value.TopicName);
        Assert.Equal("user-handler", response.Value.ForwardTo);
    }

    [Fact]
    public async Task FullMassTransitTopologyPattern_WithSlashedNames()
    {
        // Simulate MassTransit's full topology creation:
        // 1. Create receive endpoint queue
        // 2. Create topic for each message type (Namespace/Type format)
        // 3. Create subscription on topic forwarding to queue
        // 4. Create rule on subscription

        var queueName = "order-processor";
        var topicName = "AlmostServiceBus.MassTransit.Tests.Contracts/OrderPlaced";
        var subName = "order-processor";

        // Step 1: Queue
        await _adminClient.CreateQueueAsync(queueName);

        // Step 2: Topic
        await _adminClient.CreateTopicAsync(topicName);

        // Step 3: Subscription with ForwardTo
        var subOptions = new CreateSubscriptionOptions(topicName, subName)
        {
            ForwardTo = queueName
        };
        await _adminClient.CreateSubscriptionAsync(subOptions);

        // Step 4: Rule
        var ruleOptions = new CreateRuleOptions("default-rule");
        await _adminClient.CreateRuleAsync(topicName, subName, ruleOptions);

        // Verify everything exists
        Assert.True((await _adminClient.QueueExistsAsync(queueName)).Value);
        Assert.True((await _adminClient.TopicExistsAsync(topicName)).Value);
        Assert.True((await _adminClient.SubscriptionExistsAsync(topicName, subName)).Value);
    }

    [Fact]
    public async Task DeleteTopic_WithSlashInName_Succeeds()
    {
        var topicName = "My.Namespace/Events-ToDelete";
        await _adminClient.CreateTopicAsync(topicName);
        Assert.True((await _adminClient.TopicExistsAsync(topicName)).Value);

        await _adminClient.DeleteTopicAsync(topicName);

        Assert.False((await _adminClient.TopicExistsAsync(topicName)).Value);
    }
}

}
