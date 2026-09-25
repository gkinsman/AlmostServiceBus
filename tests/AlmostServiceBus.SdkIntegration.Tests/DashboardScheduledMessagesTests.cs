using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// The dashboard's scheduled-message admin routes, driven against messages the real SDK
/// scheduled with <c>ScheduleMessageAsync</c>.
/// </summary>
public class DashboardScheduledMessagesTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private readonly HttpClient _http = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _fixture.DisposeAsync();
    }

    private string Api => $"http://localhost:{_fixture.PublicPort}/api/dashboard/namespaces/{_fixture.Namespace}/scheduled";

    private ServiceBusClient CreateClient() => new(
        _fixture.ConnectionString,
        new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
        });

    [Fact]
    public async Task ListedScheduledMessage_ShiftedIntoThePast_IsReceived_AndStaysCancellableUntilThen()
    {
        const string queueName = "scheduled/admin";
        _fixture.GetNamespaceContext().CreateQueue(queueName);

        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        var seq = await sender.ScheduleMessageAsync(
            new ServiceBusMessage("later") { MessageId = "s1" },
            DateTimeOffset.UtcNow.AddDays(1));
        var cancelled = await sender.ScheduleMessageAsync(
            new ServiceBusMessage("never") { MessageId = "s2" },
            DateTimeOffset.UtcNow.AddDays(2));

        var listed = await _http.GetFromJsonAsync<JsonElement>($"{Api}?entity={Uri.EscapeDataString(queueName)}");
        var first = listed.EnumerateArray().First();
        Assert.Equal(2, listed.GetArrayLength());
        Assert.Equal(queueName, first.GetProperty("entityName").GetString());
        Assert.Equal("s1", first.GetProperty("message").GetProperty("messageId").GetString());
        Assert.Equal(seq, first.GetProperty("message").GetProperty("sequenceNumber").GetInt64());

        // A shift keeps sequence numbers, so the SDK's own cancel still finds the message.
        var shift = await _http.PostAsJsonAsync($"{Api}/shift", new { offsetSeconds = 3600 });
        Assert.Equal(HttpStatusCode.OK, shift.StatusCode);
        await sender.CancelScheduledMessageAsync(cancelled);

        shift = await _http.PostAsJsonAsync($"{Api}/shift", new { offsetSeconds = -TimeSpan.FromDays(2).TotalSeconds });
        Assert.Equal(1, (await shift.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("count").GetInt32());

        var receiver = client.CreateReceiver(queueName);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(received);
        Assert.Equal("s1", received.MessageId);
        Assert.Empty((await _http.GetFromJsonAsync<JsonElement>(Api)).EnumerateArray());
    }

    [Fact]
    public async Task DeliverAll_OnlyAffectsTheRequestedNamespace()
    {
        const string queueName = "scheduled-bulk";
        _fixture.GetNamespaceContext().CreateQueue(queueName);

        await using var client = CreateClient();
        var sender = client.CreateSender(queueName);
        var when = DateTimeOffset.UtcNow.AddDays(1);
        await sender.ScheduleMessageAsync(new ServiceBusMessage("a") { MessageId = "a" }, when);
        await sender.ScheduleMessageAsync(new ServiceBusMessage("b") { MessageId = "b" }, when);

        // The fixture's namespace is a GUID, so "default" is a different namespace: a bulk
        // delivery there must not touch this one's messages.
        Assert.NotEqual("default", _fixture.Namespace);
        var otherNs = $"http://localhost:{_fixture.PublicPort}/api/dashboard/namespaces/default/scheduled";
        var other = await _http.PostAsync($"{otherNs}/deliver", null);
        Assert.Equal(HttpStatusCode.OK, other.StatusCode);
        Assert.Equal(0, (await other.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("count").GetInt32());
        Assert.Equal(2, (await _http.GetFromJsonAsync<JsonElement>(Api)).GetArrayLength());

        var shift = await _http.PostAsJsonAsync($"{Api}/shift", new { offsetSeconds = -3600 });
        Assert.Equal(2, (await shift.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("count").GetInt32());

        var deliver = await _http.PostAsync($"{Api}/deliver", null);
        Assert.Equal(2, (await deliver.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("count").GetInt32());

        var receiver = client.CreateReceiver(queueName);
        var received = await receiver.ReceiveMessagesAsync(2, TimeSpan.FromSeconds(10));
        var ids = received.Select(m => m.MessageId).ToList();
        if (ids.Count < 2)
            ids.AddRange((await receiver.ReceiveMessagesAsync(2 - ids.Count, TimeSpan.FromSeconds(10))).Select(m => m.MessageId));
        Assert.Equal(new[] { "a", "b" }, ids.Order());

        var listed = await _http.GetFromJsonAsync<JsonElement>(Api);
        Assert.Empty(listed.EnumerateArray());
    }
}
