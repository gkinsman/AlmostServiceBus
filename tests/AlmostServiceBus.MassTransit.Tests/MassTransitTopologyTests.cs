using System.Net;
using System.Net.Sockets;
using Azure;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.MassTransit.Tests;

/// <summary>
/// Tests that prove the emulator's REST management API is compatible with the
/// Azure SDK's <see cref="ServiceBusAdministrationClient"/>, which is what MassTransit
/// uses to create topology (queues, topics, subscriptions, rules) on startup.
///
/// MassTransit's topology creation pattern is:
///   1. GET entity — check if it exists
///   2. If 404: PUT entity — create it
///   3. If 409 (conflict on create): GET entity again — another instance created it
///
/// These tests exercise that exact pattern using the real Azure SDK admin client
/// pointed at the emulator via a custom HTTP pipeline transport.
/// </summary>
public class MassTransitTopologyTests : IAsyncLifetime
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
                // The SDK builds URIs from the connection string FQDN (e.g. test-xxx.localhost:port).
                // That subdomain doesn't resolve in DNS, so we redirect the TCP connection to
                // 127.0.0.1 on the same port. The Host header still carries the original
                // hostname, which the emulator uses for namespace resolution.
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

    // ── Queue topology ──────────────────────────────────────────────────────

    [Fact]
    public async Task CreateQueue_ViaAdminClient_Succeeds()
    {
        var response = await _adminClient.CreateQueueAsync("mt-test-queue");

        Assert.Equal("mt-test-queue", response.Value.Name);
    }

    [Fact]
    public async Task GetQueue_AfterCreate_ReturnsProperties()
    {
        await _adminClient.CreateQueueAsync("mt-get-queue");

        var response = await _adminClient.GetQueueAsync("mt-get-queue");

        Assert.Equal("mt-get-queue", response.Value.Name);
    }

    [Fact]
    public async Task GetQueue_NonExistent_Throws404()
    {
        // The SDK wraps RequestFailedException in ServiceBusException
        var ex = await Assert.ThrowsAsync<Azure.Messaging.ServiceBus.ServiceBusException>(
            () => _adminClient.GetQueueAsync("does-not-exist"));

        Assert.Equal(Azure.Messaging.ServiceBus.ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact]
    public async Task QueueExists_ReturnsTrueAfterCreate()
    {
        await _adminClient.CreateQueueAsync("mt-exists-queue");

        var exists = await _adminClient.QueueExistsAsync("mt-exists-queue");

        Assert.True(exists.Value);
    }

    [Fact]
    public async Task QueueExists_ReturnsFalseWhenMissing()
    {
        var exists = await _adminClient.QueueExistsAsync("mt-missing-queue");

        Assert.False(exists.Value);
    }

    // ── Topic topology ──────────────────────────────────────────────────────

    [Fact]
    public async Task CreateTopic_ViaAdminClient_Succeeds()
    {
        var response = await _adminClient.CreateTopicAsync("mt-test-topic");

        Assert.Equal("mt-test-topic", response.Value.Name);
    }

    [Fact]
    public async Task TopicExists_ReturnsTrueAfterCreate()
    {
        await _adminClient.CreateTopicAsync("mt-topic-exists");

        var exists = await _adminClient.TopicExistsAsync("mt-topic-exists");

        Assert.True(exists.Value);
    }

    // ── Subscription topology ───────────────────────────────────────────────

    [Fact]
    public async Task CreateSubscription_ViaAdminClient_Succeeds()
    {
        await _adminClient.CreateTopicAsync("mt-sub-topic");

        var response = await _adminClient.CreateSubscriptionAsync("mt-sub-topic", "mt-sub-1");

        Assert.Equal("mt-sub-1", response.Value.SubscriptionName);
        Assert.Equal("mt-sub-topic", response.Value.TopicName);
    }

    [Fact]
    public async Task CreateSubscription_WithForwardTo_PreservesProperty()
    {
        await _adminClient.CreateTopicAsync("mt-fwd-topic");
        await _adminClient.CreateQueueAsync("mt-fwd-target");

        var subOptions = new CreateSubscriptionOptions("mt-fwd-topic", "mt-fwd-sub")
        {
            ForwardTo = "mt-fwd-target"
        };
        var response = await _adminClient.CreateSubscriptionAsync(subOptions);

        Assert.Equal("mt-fwd-target", response.Value.ForwardTo);
    }

    [Fact]
    public async Task SubscriptionExists_ReturnsTrueAfterCreate()
    {
        await _adminClient.CreateTopicAsync("mt-subexist-topic");
        await _adminClient.CreateSubscriptionAsync("mt-subexist-topic", "mt-subexist-sub");

        var exists = await _adminClient.SubscriptionExistsAsync("mt-subexist-topic", "mt-subexist-sub");

        Assert.True(exists.Value);
    }

    // ── Rule topology ───────────────────────────────────────────────────────

    [Fact]
    public async Task CreateRule_SqlFilter_ViaAdminClient_Succeeds()
    {
        await _adminClient.CreateTopicAsync("mt-rule-topic");
        await _adminClient.CreateSubscriptionAsync("mt-rule-topic", "mt-rule-sub");

        var ruleOptions = new CreateRuleOptions("color-filter")
        {
            Filter = new SqlRuleFilter("color = 'blue'")
        };
        var response = await _adminClient.CreateRuleAsync(
            "mt-rule-topic", "mt-rule-sub", ruleOptions);

        Assert.Equal("color-filter", response.Value.Name);
        Assert.IsType<SqlRuleFilter>(response.Value.Filter);
    }

    // ── MassTransit topology creation pattern ───────────────────────────────

    [Fact]
    public async Task MassTransitPattern_CreateIfNotExists_Queue()
    {
        // Simulate MassTransit's pattern: check existence, create if missing.
        var name = "mt-pattern-queue";

        var exists = await _adminClient.QueueExistsAsync(name);
        Assert.False(exists.Value);

        // Create since it doesn't exist
        await _adminClient.CreateQueueAsync(name);

        // Verify it now exists
        exists = await _adminClient.QueueExistsAsync(name);
        Assert.True(exists.Value);
    }

    [Fact]
    public async Task MassTransitPattern_CreateIfNotExists_TopicWithSubscription()
    {
        // MassTransit creates: topic -> subscription -> rule (for each consumer)
        var topicName = "mt-pattern-topic";
        var subName = "mt-pattern-sub";

        // Step 1: create topic
        var topicExists = await _adminClient.TopicExistsAsync(topicName);
        Assert.False(topicExists.Value);
        await _adminClient.CreateTopicAsync(topicName);

        // Step 2: create subscription
        var subExists = await _adminClient.SubscriptionExistsAsync(topicName, subName);
        Assert.False(subExists.Value);
        await _adminClient.CreateSubscriptionAsync(topicName, subName);

        // Verify
        topicExists = await _adminClient.TopicExistsAsync(topicName);
        Assert.True(topicExists.Value);
        subExists = await _adminClient.SubscriptionExistsAsync(topicName, subName);
        Assert.True(subExists.Value);
    }

    [Fact]
    public async Task DeleteQueue_ViaAdminClient_Works()
    {
        await _adminClient.CreateQueueAsync("mt-delete-queue");
        Assert.True((await _adminClient.QueueExistsAsync("mt-delete-queue")).Value);

        await _adminClient.DeleteQueueAsync("mt-delete-queue");

        Assert.False((await _adminClient.QueueExistsAsync("mt-delete-queue")).Value);
    }

    [Fact]
    public async Task DeleteTopic_ViaAdminClient_Works()
    {
        await _adminClient.CreateTopicAsync("mt-delete-topic");
        Assert.True((await _adminClient.TopicExistsAsync("mt-delete-topic")).Value);

        await _adminClient.DeleteTopicAsync("mt-delete-topic");

        Assert.False((await _adminClient.TopicExistsAsync("mt-delete-topic")).Value);
    }
}
