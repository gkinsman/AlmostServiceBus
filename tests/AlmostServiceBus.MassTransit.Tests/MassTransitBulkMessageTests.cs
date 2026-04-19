using System.Collections.Concurrent;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.TestHost;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;

namespace AlmostServiceBus.MassTransit.Tests;

public record BulkTestMessage(int Index);

public class BulkTestConsumer : IConsumer<BulkTestMessage>
{
    public static readonly ConcurrentBag<int> Consumed = [];
    public static int InvocationCount;

    public Task Consume(ConsumeContext<BulkTestMessage> context)
    {
        Interlocked.Increment(ref InvocationCount);

        // Deterministic: every 10th message fails
        if (context.Message.Index % 10 == 0)
            throw new InvalidOperationException($"Deliberate failure for message {context.Message.Index}");

        Consumed.Add(context.Message.Index);
        return Task.CompletedTask;
    }

}

public class MassTransitBulkMessageTests : IAsyncLifetime
{
    private const int TotalMessages = 1000;
    private const int FailureRate = 10; // index % 10 == 0 → fail → 100 failures

    // UseDevelopmentEmulator=true hardcodes AMQP to port 5672, so we must use that port.
    private readonly ServiceBusEmulatorFixture _fixture = new(publicPort: 5672);
    // Unique queue name per run to avoid stale data from standalone emulator
    private readonly string _queueName = $"bulk-test-{Guid.NewGuid():N}"[..24];
    private ServiceProvider _provider = null!;
    private IBusControl _bus = null!;

    private static bool ShouldFail(int index) => index % FailureRate == 0;

    public async Task InitializeAsync()
    {
        BulkTestConsumer.Consumed.Clear();
        BulkTestConsumer.InvocationCount = 0;
        await _fixture.StartAsync();

        var services = new ServiceCollection();
        services.AddMassTransit(x =>
        {
            x.AddConsumer<BulkTestConsumer>();

            x.UsingAzureServiceBus((ctx, cfg) =>
            {
                // UseDevelopmentEmulator=true selects plain AMQP on 5672 + plain admin HTTP on 5300
                // (MS-emulator compatibility mode — the only mode this emulator supports).
                // RootManageSharedAccessKey maps to "default" namespace in the emulator.
                var cs = "Endpoint=sb://localhost:5672;" +
                         "SharedAccessKeyName=RootManageSharedAccessKey;" +
                         "SharedAccessKey=emulator;" +
                         "UseDevelopmentEmulator=true";
                cfg.Host(cs);

                cfg.ReceiveEndpoint(_queueName, e =>
                {
                    e.UseMessageRetry(r => r.None());
                    e.ConfigureConsumer<BulkTestConsumer>(ctx);
                });
            });
        });

        _provider = services.BuildServiceProvider();
        _bus = _provider.GetRequiredService<IBusControl>();
        await _bus.StartAsync();

        // MassTransit auto-provisions topology on startup — give it a moment
        await Task.Delay(3000);
    }

    public async Task DisposeAsync()
    {
        try { await _bus.StopAsync(TimeSpan.FromSeconds(5)); } catch { }
        await _provider.DisposeAsync();
        await _fixture.DisposeAsync();
    }

    [Fact]
    public async Task BulkSend_WithDeterministicFailures_CorrectBrokerState()
    {
        var expectedSuccessCount = 0;
        var expectedFailureCount = 0;

        // Send 1000 messages — 10% will fail deterministically
        var sendEndpoint = await _bus.GetSendEndpoint(new Uri($"queue:{_queueName}"));

        for (var i = 0; i < TotalMessages; i++)
        {
            if (ShouldFail(i)) expectedFailureCount++;
            else expectedSuccessCount++;

            await sendEndpoint.Send(new BulkTestMessage(i));
        }

        Assert.Equal(900, expectedSuccessCount);
        Assert.Equal(100, expectedFailureCount);

        // Wait for all messages to be consumed + errors to land.
        // MassTransit moves faulted messages to _error queue after retries exhausted.
        var deadline = DateTime.UtcNow.AddSeconds(120);
        while (DateTime.UtcNow < deadline)
        {
            var consumed = BulkTestConsumer.Consumed.Count;
            var errorQueue = FindErrorQueue();
            var errorCount = errorQueue?.TotalMessageCount ?? 0;

            if (consumed >= expectedSuccessCount && errorCount >= expectedFailureCount)
                break;

            await Task.Delay(500);
        }

        // ── Assert consumer received all success messages ──
        Assert.True(BulkTestConsumer.InvocationCount > 0, "Consumer was never invoked");
        var consumedIndices = BulkTestConsumer.Consumed.ToHashSet();
        Assert.Equal(expectedSuccessCount, consumedIndices.Count);

        // Verify the RIGHT messages were consumed (not the failing ones)
        for (var i = 0; i < TotalMessages; i++)
        {
            if (ShouldFail(i))
                Assert.DoesNotContain(i, consumedIndices);
            else
                Assert.Contains(i, consumedIndices);
        }

        // ── Assert broker state ──
        var ns = _fixture.GetDefaultNamespaceContext();
        var queue = ns.GetQueue(_queueName);
        Assert.NotNull(queue);

        // All messages should be settled (none left in the queue)
        Assert.Equal(0, queue!.MessageCount);

        // All 1000 messages were settled from the source queue — MassTransit accepts
        // even faulted messages, then sends a copy to the _error queue.
        Assert.Equal(TotalMessages, queue.ConsumedCount);

        // All failures in the error queue
        var errQueue = FindErrorQueue();
        Assert.NotNull(errQueue);
        Assert.Equal(expectedFailureCount, errQueue!.TotalMessageCount);
    }

    private QueueEntity? FindErrorQueue()
    {
        // UseDevelopmentEmulator uses "default" namespace
        var ns = _fixture.GetDefaultNamespaceContext();
        return ns.GetQueue($"{_queueName}_error");
    }
}
