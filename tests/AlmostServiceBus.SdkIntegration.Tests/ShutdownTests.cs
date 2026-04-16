using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Tests that verify graceful shutdown of Azure SDK clients completes quickly.
/// The Azure SDK's ServiceBusClient uses AMQPS (TLS) and performs a graceful
/// AMQP connection close handshake. If the emulator doesn't handle this properly,
/// shutdown takes 30+ seconds instead of being near-instant.
/// </summary>
public class ShutdownTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    private ServiceBusClient CreateServiceBusClient()
    {
        var connectionString =
            $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";

        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions
            {
                MaxRetries = 0,
                TryTimeout = TimeSpan.FromSeconds(5)
            }
        };

        return new ServiceBusClient(connectionString, clientOptions);
    }

    [Fact]
    public async Task ProcessorStopAndClientDispose_CompletesWithinFiveSeconds()
    {
        // Arrange: create queue and set up processor
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("shutdown-test-queue");

        await using var client = CreateServiceBusClient();

        var processor = client.CreateProcessor("shutdown-test-queue", new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = true,
            MaxConcurrentCalls = 1,
            PrefetchCount = 0
        });

        var messageReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        processor.ProcessMessageAsync += args =>
        {
            messageReceived.TrySetResult(true);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        // Act: start processor, send a message, wait for receipt
        await processor.StartProcessingAsync();

        await using var sender = client.CreateSender("shutdown-test-queue");
        await sender.SendMessageAsync(new ServiceBusMessage("shutdown-test-payload"));

        // Wait for the message to be received (or timeout)
        var received = await Task.WhenAny(messageReceived.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.True(messageReceived.Task.IsCompletedSuccessfully, "Message was not received within 10 seconds");

        // Act: measure shutdown time
        var sw = Stopwatch.StartNew();
        await processor.StopProcessingAsync();
        await processor.DisposeAsync();
        // Sender was disposed via await using above, but let's be explicit
        sw.Stop();

        // Assert: shutdown should be fast
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Processor stop took {sw.Elapsed.TotalSeconds:F1}s — expected less than 5s");
    }

    [Fact]
    public async Task ReceiverClose_CompletesWithinFiveSeconds()
    {
        // Simpler test: just open a receiver, then close it
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("shutdown-receiver-queue");

        await using var client = CreateServiceBusClient();
        var receiver = client.CreateReceiver("shutdown-receiver-queue");

        // Open the link by attempting a receive (will timeout quickly since queue is empty)
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
        Assert.Null(msg); // Queue is empty, that's fine

        // Now measure how long close takes
        var sw = Stopwatch.StartNew();
        await receiver.CloseAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Receiver close took {sw.Elapsed.TotalSeconds:F1}s — expected less than 5s");
    }

    [Fact]
    public async Task ClientDispose_WithActiveReceiver_CompletesWithinFiveSeconds()
    {
        // Test disposing the entire client while a receiver is active
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("shutdown-dispose-queue");

        var client = CreateServiceBusClient();
        var receiver = client.CreateReceiver("shutdown-dispose-queue");

        // Open the link
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
        Assert.Null(msg);

        // Dispose the client (should close all links/connections)
        var sw = Stopwatch.StartNew();
        await client.DisposeAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Client dispose took {sw.Elapsed.TotalSeconds:F1}s — expected less than 5s");
    }

    [Fact]
    public async Task SenderClose_CompletesWithinFiveSeconds()
    {
        // Even sender links need graceful close
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("shutdown-sender-queue");

        await using var client = CreateServiceBusClient();
        var sender = client.CreateSender("shutdown-sender-queue");

        // Send a message to open the link
        await sender.SendMessageAsync(new ServiceBusMessage("test"));

        // Close sender
        var sw = Stopwatch.StartNew();
        await sender.CloseAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Sender close took {sw.Elapsed.TotalSeconds:F1}s — expected less than 5s");
    }

    [Fact]
    public async Task MultipleProcessors_ConcurrentShutdown_CompletesWithinFiveSeconds()
    {
        // More realistic: multiple processors on different queues, all shutting down together.
        // This is what happens when a MassTransit bus host stops — it has multiple receive
        // endpoints, each with their own processor, and they all close at once.
        var context = _fixture.GetDefaultNamespaceContext();
        var queueNames = new[] { "multi-shutdown-q1", "multi-shutdown-q2", "multi-shutdown-q3" };
        foreach (var q in queueNames)
            context.CreateQueue(q);

        await using var client = CreateServiceBusClient();

        var processors = new List<ServiceBusProcessor>();
        var receivedCounts = new int[queueNames.Length];

        for (int i = 0; i < queueNames.Length; i++)
        {
            var idx = i;
            var processor = client.CreateProcessor(queueNames[i], new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = true,
                MaxConcurrentCalls = 1,
                PrefetchCount = 0
            });

            processor.ProcessMessageAsync += args =>
            {
                Interlocked.Increment(ref receivedCounts[idx]);
                return Task.CompletedTask;
            };
            processor.ProcessErrorAsync += args => Task.CompletedTask;

            processors.Add(processor);
        }

        // Start all processors
        foreach (var p in processors)
            await p.StartProcessingAsync();

        // Send a message to each queue
        for (int i = 0; i < queueNames.Length; i++)
        {
            await using var sender = client.CreateSender(queueNames[i]);
            await sender.SendMessageAsync(new ServiceBusMessage($"msg-{i}"));
        }

        // Wait for at least one message per queue
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline && receivedCounts.Any(c => c == 0))
            await Task.Delay(100);

        Assert.All(receivedCounts, c => Assert.True(c > 0, "Expected at least one message per queue"));

        // Stop all processors concurrently and measure total time
        var sw = Stopwatch.StartNew();
        await Task.WhenAll(processors.Select(p => p.StopProcessingAsync()));
        foreach (var p in processors)
            await p.DisposeAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Concurrent processor shutdown took {sw.Elapsed.TotalSeconds:F1}s — expected less than 5s");
    }

    [Fact]
    public async Task ProcessorWithSustainedMessageFlow_ShutdownCompletesWithinFiveSeconds()
    {
        // Pump messages continuously, then stop mid-flow. This exercises
        // the drain path with messages potentially in-flight.
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("sustained-shutdown-queue");

        await using var client = CreateServiceBusClient();

        var processor = client.CreateProcessor("sustained-shutdown-queue", new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = true,
            MaxConcurrentCalls = 1,
            PrefetchCount = 0
        });

        var receivedCount = 0;
        processor.ProcessMessageAsync += args =>
        {
            Interlocked.Increment(ref receivedCount);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();

        // Send a burst of messages
        await using var sender = client.CreateSender("sustained-shutdown-queue");
        for (int i = 0; i < 20; i++)
            await sender.SendMessageAsync(new ServiceBusMessage($"burst-{i}"));

        // Wait for some to be received
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline && receivedCount < 5)
            await Task.Delay(100);

        // Now stop while messages may still be flowing
        var sw = Stopwatch.StartNew();
        await processor.StopProcessingAsync();
        await processor.DisposeAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Processor stop during sustained flow took {sw.Elapsed.TotalSeconds:F1}s — expected less than 5s");
    }
}
