using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// The dashboard API has one route per operation. Entity names can contain slashes, so the
/// dashboard sends them percent-encoded as a single path segment. These tests pin both halves:
/// an encoded slashed name reaches the right entity, and a raw slashed path is simply not a route.
/// </summary>
public class DashboardApiRoutingTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private readonly HttpClient _http = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _fixture.DisposeAsync();
    }

    private string Api => $"http://localhost:{_fixture.PublicPort}/api/dashboard/namespaces/{_fixture.Namespace}";

    private ServiceBusClient CreateClient() => new(
        _fixture.ConnectionString,
        new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
        });

    [Fact]
    public async Task SlashedQueueName_IsAddressedAsOneEncodedSegment()
    {
        const string queueName = "orders/eu/high";
        _fixture.GetNamespaceContext().CreateQueue(queueName);

        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        await sender.SendMessageAsync(new ServiceBusMessage("body") { MessageId = "m1" });

        var encoded = Uri.EscapeDataString(queueName);
        var messages = await _http.GetFromJsonAsync<JsonElement>($"{Api}/queues/{encoded}/messages");
        Assert.Equal("m1", messages.EnumerateArray().Single().GetProperty("messageId").GetString());

        var properties = await _http.GetFromJsonAsync<JsonElement>($"{Api}/queues/{encoded}/properties");
        Assert.Equal(queueName, properties.GetProperty("name").GetString());

        // The raw slashed path is three segments and matches no route.
        var raw = await _http.GetAsync($"{Api}/queues/{queueName}/messages");
        Assert.Equal(HttpStatusCode.NotFound, raw.StatusCode);

        // Purge, then nothing is left to receive.
        var purge = await _http.DeleteAsync($"{Api}/queues/{encoded}/messages");
        Assert.Equal(HttpStatusCode.OK, purge.StatusCode);
        var after = await _http.GetFromJsonAsync<JsonElement>($"{Api}/queues/{encoded}/messages");
        // Peek lists recently settled messages too, so check that nothing is still active.
        Assert.DoesNotContain(after.EnumerateArray(), m => m.GetProperty("state").GetString() == "Active");
    }

    [Fact]
    public async Task SubscriptionRoutes_ResolveTopicAndSubscriptionSegments()
    {
        const string topicName = "events/orders";
        const string subscriptionName = "billing";
        _fixture.GetNamespaceContext().CreateSubscription(topicName, subscriptionName);

        await using var client = CreateClient();
        var sender = client.CreateSender(topicName);
        await sender.SendMessageAsync(new ServiceBusMessage("evt") { MessageId = "e1" });

        var topic = Uri.EscapeDataString(topicName);
        var subscription = $"{Api}/topics/{topic}/subscriptions/{subscriptionName}";

        var live = await _http.GetFromJsonAsync<JsonElement>($"{subscription}/messages");
        Assert.Equal("e1", live.EnumerateArray().Single().GetProperty("messageId").GetString());

        // The topic view aggregates its subscriptions.
        var aggregated = await _http.GetFromJsonAsync<JsonElement>($"{Api}/topics/{topic}/messages");
        Assert.Single(aggregated.EnumerateArray());

        var receiver = client.CreateReceiver(topicName, subscriptionName);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        await receiver.DeadLetterMessageAsync(received, "Rejected", "test");

        var dlq = await _http.GetFromJsonAsync<JsonElement>($"{subscription}/deadletter");
        var dead = dlq.EnumerateArray().Single();
        Assert.Equal("e1", dead.GetProperty("messageId").GetString());
        Assert.Equal("Rejected", dead.GetProperty("deadLetterReason").GetString());

        Assert.Equal(HttpStatusCode.OK, (await _http.DeleteAsync($"{subscription}/deadletter")).StatusCode);
        var remaining = await _http.GetFromJsonAsync<JsonElement>($"{subscription}/deadletter");
        Assert.DoesNotContain(remaining.EnumerateArray(), m => m.GetProperty("state").GetString() == "Active");
    }

    [Fact]
    public async Task UnknownNamespaceEntityOrSubscription_Is404()
    {
        _fixture.GetNamespaceContext().CreateQueue("exists");
        _fixture.GetNamespaceContext().CreateSubscription("topic", "sub");

        var cases = new[]
        {
            $"http://localhost:{_fixture.PublicPort}/api/dashboard/namespaces/no-such-ns/queues/exists/messages",
            $"{Api}/queues/missing/messages",
            $"{Api}/queues/missing/deadletter",
            $"{Api}/queues/missing/properties",
            $"{Api}/topics/missing/messages",
            $"{Api}/topics/topic/subscriptions/missing/messages",
            $"{Api}/topics/topic/subscriptions/missing/deadletter",
        };

        foreach (var url in cases)
        {
            Assert.Equal(HttpStatusCode.NotFound, (await _http.GetAsync(url)).StatusCode);
        }
    }
}
