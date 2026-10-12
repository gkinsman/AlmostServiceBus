using System.Net;
using System.Net.Sockets;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

// Entities are created through the admin client so ForwardTo takes the management API path,
// which is where gkinsman/AlmostServiceBus#158 lost subscription -> topic forwards.
public class AutoForwardingTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private ServiceBusAdministrationClient _admin = null!;
    private ServiceBusClient _client = null!;

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();

        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (context, ct) =>
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(IPAddress.Loopback, context.DnsEndPoint.Port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
        };
        _admin = new ServiceBusAdministrationClient(
            _fixture.ConnectionString,
            new ServiceBusAdministrationClientOptions { Transport = new HttpClientTransport(new HttpClient(handler)) });

        _client = new ServiceBusClient(_fixture.ConnectionString, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
        });
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _fixture.DisposeAsync();
    }

    [Fact]
    public async Task SubscriptionForwardingToTopic_IsDelivered()
    {
        await _admin.CreateTopicAsync("fwd/source");
        await _admin.CreateTopicAsync("fwd/target");
        await _admin.CreateSubscriptionAsync("fwd/target", "counter");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("fwd/source", "fwd") { ForwardTo = "fwd/target" });

        await _client.CreateSender("fwd/source").SendMessageAsync(new ServiceBusMessage("x"));

        var delivered = await _client.CreateReceiver("fwd/target", "counter").ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(delivered);
        Assert.Equal("x", delivered!.Body.ToString());

        var source = await _admin.GetSubscriptionRuntimePropertiesAsync("fwd/source", "fwd");
        Assert.Equal(0, source.Value.ActiveMessageCount);
    }

    [Fact]
    public async Task QueueForwardingToTopic_IsDelivered()
    {
        await _admin.CreateTopicAsync("queue-fwd-target");
        await _admin.CreateSubscriptionAsync("queue-fwd-target", "sub");
        await _admin.CreateQueueAsync(new CreateQueueOptions("queue-fwd-source") { ForwardTo = "queue-fwd-target" });

        await _client.CreateSender("queue-fwd-source").SendMessageAsync(new ServiceBusMessage("x"));

        var delivered = await _client.CreateReceiver("queue-fwd-target", "sub").ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(delivered);
    }

    [Fact]
    public async Task DeletedDestination_LandsInTheSourcesDeadLetterQueue()
    {
        await _admin.CreateQueueAsync("tdlq-target");
        await _admin.CreateTopicAsync("tdlq-source");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("tdlq-source", "fwd") { ForwardTo = "tdlq-target" });
        await _admin.DeleteQueueAsync("tdlq-target");

        await _client.CreateSender("tdlq-source").SendMessageAsync(new ServiceBusMessage("x") { MessageId = "lost" });

        var props = await _admin.GetSubscriptionRuntimePropertiesAsync("tdlq-source", "fwd");
        Assert.Equal(1, props.Value.DeadLetterMessageCount);

        var receiver = _client.CreateReceiver("tdlq-source", "fwd",
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var dead = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dead);
        Assert.Equal("lost", dead!.MessageId);
        Assert.StartsWith("The transfer destination entity does not exist.", dead.DeadLetterReason);
    }

    [Fact]
    public async Task RemovingForwardTo_KeepsMessagesInTheSubscription()
    {
        await _admin.CreateQueueAsync("unfwd-target");
        await _admin.CreateTopicAsync("unfwd-source");
        var sub = await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("unfwd-source", "fwd") { ForwardTo = "unfwd-target" });
        sub.Value.ForwardTo = null;
        await _admin.UpdateSubscriptionAsync(sub.Value);

        await _client.CreateSender("unfwd-source").SendMessageAsync(new ServiceBusMessage("x"));

        var kept = await _client.CreateReceiver("unfwd-source", "fwd").ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(kept);
    }
}
