using System.Net;
using System.Net.Sockets;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

public class AdminClientTests : IAsyncLifetime
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
    public async Task CreateQueue_ThenGetQueue_RoundTrips()
    {
        var created = await _adminClient.CreateQueueAsync("test-queue");
        Assert.Equal("test-queue", created.Value.Name);

        var fetched = await _adminClient.GetQueueAsync("test-queue");
        Assert.Equal("test-queue", fetched.Value.Name);
    }

    [Fact]
    public async Task CreateTopicAndSubscription_Works()
    {
        await _adminClient.CreateTopicAsync("my-topic");

        var subOptions = new CreateSubscriptionOptions("my-topic", "sub-1")
        {
            ForwardTo = "some-queue"
        };
        await _adminClient.CreateQueueAsync("some-queue");
        var sub = await _adminClient.CreateSubscriptionAsync(subOptions);

        Assert.Equal("sub-1", sub.Value.SubscriptionName);
        Assert.Equal("some-queue", sub.Value.ForwardTo);
    }

    [Fact]
    public async Task GetNonexistentEntity_Throws404()
    {
        var ex = await Assert.ThrowsAsync<Azure.Messaging.ServiceBus.ServiceBusException>(
            () => _adminClient.GetQueueAsync("nonexistent"));

        Assert.Equal(Azure.Messaging.ServiceBus.ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact]
    public async Task CreateSubscriptionWithRules_Works()
    {
        await _adminClient.CreateTopicAsync("rules-topic");
        await _adminClient.CreateSubscriptionAsync("rules-topic", "rules-sub");

        var ruleOptions = new CreateRuleOptions("my-rule")
        {
            Filter = new SqlRuleFilter("color = 'blue'")
        };
        var rule = await _adminClient.CreateRuleAsync("rules-topic", "rules-sub", ruleOptions);

        Assert.Equal("my-rule", rule.Value.Name);
        Assert.IsType<SqlRuleFilter>(rule.Value.Filter);
    }

    [Fact]
    public async Task DeleteEntity_Works()
    {
        await _adminClient.CreateQueueAsync("delete-me");
        Assert.True((await _adminClient.QueueExistsAsync("delete-me")).Value);

        await _adminClient.DeleteQueueAsync("delete-me");
        Assert.False((await _adminClient.QueueExistsAsync("delete-me")).Value);
    }

    [Fact]
    public async Task UpdateSubscription_WithIfMatch_Works()
    {
        await _adminClient.CreateTopicAsync("update-topic");
        await _adminClient.CreateSubscriptionAsync("update-topic", "update-sub");

        var sub = await _adminClient.GetSubscriptionAsync("update-topic", "update-sub");
        sub.Value.MaxDeliveryCount = 5;
        var updated = await _adminClient.UpdateSubscriptionAsync(sub.Value);

        Assert.Equal(5, updated.Value.MaxDeliveryCount);
    }

    [Fact]
    public async Task CreateSubscription_WithDefaultRule_ReplacesDefaultTrueFilter()
    {
        // This exercises the Azure SDK overload that embeds DefaultRuleDescription in the
        // subscription XML (CreateSubscriptionAsync(options, CreateRuleOptions)).
        await _adminClient.CreateTopicAsync("default-rule-topic");

        var ruleOptions = new CreateRuleOptions(Guid.NewGuid().ToString())
        {
            Filter = new SqlRuleFilter("ClientId = 27")
        };
        await _adminClient.CreateSubscriptionAsync(
            new CreateSubscriptionOptions("default-rule-topic", "default-rule-sub"),
            ruleOptions);

        var rules = await _adminClient.GetRulesAsync("default-rule-topic", "default-rule-sub")
            .ToListAsync();

        Assert.Single(rules);
        var filter = Assert.IsType<SqlRuleFilter>(rules[0].Filter);
        Assert.Equal("ClientId = 27", filter.SqlExpression);
    }

    [Fact]
    public async Task UpdateRule_SqlFilter_PersistsNewExpression()
    {
        // Verifies that PUT rule correctly updates an existing rule's filter expression.
        await _adminClient.CreateTopicAsync("update-rule-topic");
        await _adminClient.CreateSubscriptionAsync("update-rule-topic", "update-rule-sub");

        var rules = await _adminClient.GetRulesAsync("update-rule-topic", "update-rule-sub")
            .ToListAsync();
        Assert.Single(rules);

        var existingRule = rules[0];
        existingRule.Filter = new SqlRuleFilter("0 = 1");
        await _adminClient.UpdateRuleAsync("update-rule-topic", "update-rule-sub", existingRule);

        var updatedRules = await _adminClient.GetRulesAsync("update-rule-topic", "update-rule-sub")
            .ToListAsync();
        Assert.Single(updatedRules);
        var updatedFilter = Assert.IsType<SqlRuleFilter>(updatedRules[0].Filter);
        Assert.Equal("0 = 1", updatedFilter.SqlExpression);
    }
}
