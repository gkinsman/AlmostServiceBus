using System.Net.Http.Json;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Dead-lettered messages as seen through the SDK's dead-letter sub-queue and through the
/// dashboard API. Explicitly dead-lettered messages used to be invisible to both.
/// </summary>
public class DeadLetterVisibilityTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private readonly HttpClient _http = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _fixture.DisposeAsync();
    }

    private ServiceBusClient CreateClient() => new(
        _fixture.ConnectionString,
        new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
        });

    // The public port multiplexes HTTP to the same Kestrel that serves the dashboard API.
    private string DashboardApi => $"http://localhost:{_fixture.PublicPort}/api/dashboard";

    [Fact]
    public async Task ExplicitAndMaxDeliveryDeadLetters_AreBothPeekable()
    {
        const string queueName = "dlq-visibility";
        var queue = _fixture.GetNamespaceContext().CreateQueue(queueName);
        queue.MaxDeliveryCount = 2;

        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        for (var i = 1; i <= 2; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"body-{i}")
            {
                MessageId = $"m{i}",
                Subject = "Demo",
                ApplicationProperties = { ["source"] = "test", ["n"] = i },
            });
        }

        var receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions { PrefetchCount = 0 });

        // m1: explicit dead-letter with a reason.
        var m1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(m1);
        await receiver.DeadLetterMessageAsync(m1, "ValidationFailed", "schema mismatch");

        // m2: abandon until MaxDeliveryCount (2) is exceeded.
        for (var attempt = 0; attempt < 2; attempt++)
        {
            var m2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(m2);
            await receiver.AbandonMessageAsync(m2);
        }
        await Task.Delay(500);

        // SDK view of the dead-letter sub-queue.
        var dlq = client.CreateReceiver(queueName, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var peeked = await dlq.PeekMessagesAsync(10);
        var byId = peeked.ToDictionary(p => p.MessageId);
        Assert.Equal(2, byId.Count);
        Assert.Equal("ValidationFailed", byId["m1"].DeadLetterReason);
        Assert.Equal("schema mismatch", byId["m1"].DeadLetterErrorDescription);
        Assert.Equal("MaxDeliveryCountExceeded", byId["m2"].DeadLetterReason);
        Assert.Equal("test", byId["m1"].ApplicationProperties["source"]);

        // Nothing dead-lettered should still look live on the source queue.
        var source = client.CreateReceiver(queueName);
        Assert.Empty(await source.PeekMessagesAsync(10));

        // Dashboard API view of the same queue.
        var dashboardDlq = await _http.GetFromJsonAsync<JsonElement>($"{DashboardApi}/namespaces/{_fixture.Namespace}/queues/{queueName}/deadletter");
        var rows = dashboardDlq.EnumerateArray().ToDictionary(r => r.GetProperty("messageId").GetString()!);
        Assert.Equal(2, rows.Count);
        Assert.Equal("ValidationFailed", rows["m1"].GetProperty("deadLetterReason").GetString());
        Assert.Equal("schema mismatch", rows["m1"].GetProperty("deadLetterErrorDescription").GetString());
        Assert.Equal(queueName, rows["m1"].GetProperty("deadLetterSource").GetString());
        Assert.Equal("Active", rows["m1"].GetProperty("state").GetString());
        Assert.Equal("test", rows["m1"].GetProperty("applicationProperties").GetProperty("source").GetString());

        var dashboardLive = await _http.GetFromJsonAsync<JsonElement>($"{DashboardApi}/namespaces/{_fixture.Namespace}/queues/{queueName}/messages");
        Assert.All(dashboardLive.EnumerateArray(), r => Assert.Equal("DeadLettered", r.GetProperty("state").GetString()));

        var entities = await _http.GetFromJsonAsync<JsonElement>($"{DashboardApi}/namespaces/{_fixture.Namespace}/entities");
        var q = entities.GetProperty("queues").EnumerateArray().Single(e => e.GetProperty("name").GetString() == queueName);
        Assert.Equal(2, q.GetProperty("deadLetterCount").GetInt32());
        Assert.Equal(0, q.GetProperty("messageCount").GetInt32());
    }

    [Fact]
    public async Task QueueProperties_Endpoint_ReflectsConfiguration()
    {
        const string queueName = "props-queue";
        var queue = _fixture.GetNamespaceContext().CreateQueue(queueName);
        queue.RequiresSession = true;
        queue.LockDuration = TimeSpan.FromMinutes(5);
        queue.MaxDeliveryCount = 7;
        queue.DefaultMessageTimeToLive = TimeSpan.FromDays(14);
        queue.RequiresDuplicateDetection = true;
        queue.ForwardDeadLetteredMessagesTo = "poison";

        await using var client = CreateClient();
        await client.CreateSender(queueName).SendMessageAsync(new ServiceBusMessage("x") { SessionId = "s1" });

        var props = await _http.GetFromJsonAsync<JsonElement>($"{DashboardApi}/namespaces/{_fixture.Namespace}/queues/{queueName}/properties");
        Assert.Equal(queueName, props.GetProperty("name").GetString());
        Assert.Equal("PT5M", props.GetProperty("lockDuration").GetString());
        Assert.Equal(7, props.GetProperty("maxDeliveryCount").GetInt32());
        Assert.True(props.GetProperty("requiresSession").GetBoolean());
        Assert.Equal("P14D", props.GetProperty("defaultMessageTimeToLive").GetString());
        Assert.True(props.GetProperty("requiresDuplicateDetection").GetBoolean());
        Assert.Equal("PT10M", props.GetProperty("duplicateDetectionHistoryTimeWindow").GetString());
        Assert.Equal("poison", props.GetProperty("forwardDeadLetteredMessagesTo").GetString());
        Assert.Equal(JsonValueKind.Null, props.GetProperty("autoDeleteOnIdle").ValueKind);
        Assert.Equal(1, props.GetProperty("messageCount").GetInt32());
        Assert.Equal(1, props.GetProperty("sessionCount").GetInt32());

        var unbounded = _fixture.GetNamespaceContext().CreateQueue("plain");
        var plain = await _http.GetFromJsonAsync<JsonElement>($"{DashboardApi}/namespaces/{_fixture.Namespace}/queues/plain/properties");
        Assert.Equal(JsonValueKind.Null, plain.GetProperty("defaultMessageTimeToLive").ValueKind);
        Assert.Equal("PT30S", plain.GetProperty("lockDuration").GetString());
        Assert.Equal(unbounded.MaxDeliveryCount, plain.GetProperty("maxDeliveryCount").GetInt32());

        var missing = await _http.GetAsync($"{DashboardApi}/namespaces/{_fixture.Namespace}/queues/does-not-exist/properties");
        Assert.Equal(System.Net.HttpStatusCode.NotFound, missing.StatusCode);
    }
}
