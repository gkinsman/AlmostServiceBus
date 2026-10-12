using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

// A receiver whose ReceiveMessagesAsync has returned empty but which was never disposed keeps its
// link credit. Messages pumped to it come back from the SDK as AMQP `released`. Wolverine's queue
// purge leaves such receivers behind on every queue it purges, so a request/reply round trip
// bounced off several of them and missed Wolverine's 5s remote-invocation timeout
// (Wolverine end_to_end_with_named_broker.correct_scheme_on_reply_uri).
public class IdleReceiverReleaseTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task MessageReleasedByIdleReceivers_ReachesTheActiveReceiverPromptly()
    {
        const string queueName = "idle-receiver-release";
        _fixture.GetNamespaceContext().CreateQueue(queueName);

        await using var client = new ServiceBusClient(_fixture.ConnectionString);

        var idle = new List<ServiceBusReceiver>();
        for (var i = 0; i < 4; i++)
        {
            var receiver = client.CreateReceiver(queueName);
            Assert.Empty(await receiver.ReceiveMessagesAsync(25, TimeSpan.FromMilliseconds(200)));
            idle.Add(receiver);
        }

        var active = client.CreateReceiver(queueName);
        var receiveTask = active.ReceiveMessageAsync(TimeSpan.FromSeconds(20));

        await client.CreateSender(queueName).SendMessageAsync(new ServiceBusMessage("ping"));
        var stopwatch = Stopwatch.StartNew();
        var received = await receiveTask;
        stopwatch.Stop();

        Assert.NotNull(received);
        Assert.Equal("ping", received.Body.ToString());
        Assert.Equal(1, received.DeliveryCount);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(2),
            $"Message took {stopwatch.Elapsed.TotalSeconds:F1}s to reach the active receiver");
        await active.CompleteMessageAsync(received);

        foreach (var receiver in idle) await receiver.DisposeAsync();
    }
}
