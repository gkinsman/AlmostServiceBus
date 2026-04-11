using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Tests session queues through the TLS proxy path (full host with TcpMultiplexer),
/// reproducing the "OnDetach not valid under state: Start" error seen in the OrderFlowDemo.
///
/// Unlike SessionLoadTests which uses the in-process TestHost (no TLS), this test
/// starts the full emulator host on a real port with TLS termination.
/// </summary>
public class SessionTlsLoadTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    // The fixture runs on a random port WITH the TLS multiplexer
    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    /// <summary>
    /// Creates a client that goes through AMQPS (TLS), matching what MassTransit does.
    /// Default transport is AMQPS (TLS) — no explicit TransportType set.
    /// </summary>
    private ServiceBusClient CreateTlsClient()
    {
        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";
        return new ServiceBusClient(cs, new ServiceBusClientOptions
        {
            // Default = AmqpTcp with TLS (AMQPS). Don't set TransportType to force TLS path.
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 3, TryTimeout = TimeSpan.FromSeconds(30) }
        });
    }

    /// <summary>
    /// Creates a client using plain AMQP (no TLS) for comparison.
    /// </summary>
    private ServiceBusClient CreatePlainClient()
    {
        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator";
        return new ServiceBusClient(cs, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 3, TryTimeout = TimeSpan.FromSeconds(30) }
        });
    }

    /// <summary>
    /// Reproduces the exact OrderFlowDemo pattern: many session IDs, concurrent processing,
    /// sustained load over time. Messages keep arriving while processing is ongoing.
    /// </summary>
    [Fact]
    public async Task SessionProcessor_SustainedConcurrentLoad_NoErrors()
    {
        var queueName = "sustained-session-load";
        var context = _fixture.GetDefaultNamespaceContext();
        var queue = context.CreateQueue(queueName);
        queue.RequiresSession = true;
        queue.LockDuration = TimeSpan.FromSeconds(30);

        const int sessionCount = 50;
        const int waves = 40;
        const int messagesPerWave = 50;
        const int totalMessages = waves * messagesPerWave; // 2000

        var sessionIds = Enumerable.Range(0, sessionCount).Select(i => $"warehouse-{i}").ToArray();

        await using var client = CreateTlsClient();

        var completed = new ConcurrentBag<string>();
        var errors = new ConcurrentBag<(string Source, Exception Error)>();
        var allDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Start processor BEFORE sending — it should handle messages as they arrive
        await using var processor = client.CreateSessionProcessor(queueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 10,
            MaxConcurrentCallsPerSession = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(2),
            SessionIdleTimeout = TimeSpan.FromSeconds(5)
        });

        processor.ProcessMessageAsync += async args =>
        {
            // Simulate processing work (10-50ms like the demo consumers)
            await Task.Delay(Random.Shared.Next(10, 50));
            await args.CompleteMessageAsync(args.Message);
            completed.Add(args.Message.Body.ToString());

            if (completed.Count >= totalMessages)
                allDone.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            errors.Add((args.ErrorSource.ToString(), args.Exception));
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        // Send in waves with delays between them — sustained load, not a burst
        var sender = client.CreateSender(queueName);
        var msgIndex = 0;
        for (var wave = 0; wave < waves; wave++)
        {
            var tasks = new List<Task>();
            for (var i = 0; i < messagesPerWave; i++)
            {
                var sessionId = sessionIds[msgIndex % sessionCount];
                tasks.Add(sender.SendMessageAsync(new ServiceBusMessage($"order-{msgIndex}")
                {
                    SessionId = sessionId,
                    Subject = "ShipOrder"
                }));
                msgIndex++;
            }
            await Task.WhenAll(tasks);

            // Short pause between waves — sustained high throughput
            await Task.Delay(50);
        }
        await sender.CloseAsync();

        // Wait for all messages to be processed
        var result = await Task.WhenAny(allDone.Task, Task.Delay(TimeSpan.FromSeconds(120)));

        await processor.StopProcessingAsync();

        var onDetachErrors = errors.Where(e => e.Error.Message.Contains("OnDetach")).ToList();
        var socketErrors = errors.Where(e =>
            e.Error is System.Net.Sockets.SocketException ||
            e.Error.InnerException is System.Net.Sockets.SocketException).ToList();

        // Log all errors for diagnostics
        if (errors.Count > 0)
        {
            var summary = errors.GroupBy(e => e.Error.GetType().Name + ": " + e.Error.Message.Split('\n')[0])
                .Select(g => $"  [{g.Count()}x] {g.Key}")
                .ToList();
            var errorReport = string.Join("\n", summary);
            Assert.Fail($"Completed {completed.Count}/{totalMessages}. Errors:\n{errorReport}");
        }

        Assert.True(result == allDone.Task,
            $"Timed out. Completed {completed.Count}/{totalMessages}. Errors: {errors.Count}");
    }

    /// <summary>
    /// Simulates the multi-service demo topology: separate sender and receiver clients
    /// (like OrderApi + FulfillmentWorker), multiple non-session queues plus a session queue,
    /// all hitting the emulator concurrently through TLS.
    /// </summary>
    [Fact]
    public async Task MultiClient_SessionAndNonSession_ConcurrentLoad()
    {
        var context = _fixture.GetDefaultNamespaceContext();

        // Create topology matching the demo
        var sessionQueue = context.CreateQueue("logistics-dispatch-test");
        sessionQueue.RequiresSession = true;
        sessionQueue.LockDuration = TimeSpan.FromSeconds(30);

        context.CreateQueue("pick-order-test");
        context.CreateQueue("reserve-inventory-test");

        const int orderCount = 500;
        const int sessionCount = 15; // warehouses

        // Two separate clients (like OrderApi + FulfillmentWorker)
        await using var senderClient = CreateTlsClient();
        await using var receiverClient = CreateTlsClient();

        var sessionCompleted = new ConcurrentBag<string>();
        var nonSessionCompleted = new ConcurrentBag<string>();
        var errors = new ConcurrentBag<(string Source, Exception Error)>();

        // Non-session processors (simulating ReserveInventory, PickOrder consumers)
        var pickProcessor = receiverClient.CreateProcessor("pick-order-test", new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 5,
            AutoCompleteMessages = false,
        });
        pickProcessor.ProcessMessageAsync += async args =>
        {
            await Task.Delay(Random.Shared.Next(5, 20));
            await args.CompleteMessageAsync(args.Message);
            nonSessionCompleted.Add("pick");
        };
        pickProcessor.ProcessErrorAsync += args => { errors.Add(("pick", args.Exception)); return Task.CompletedTask; };

        var reserveProcessor = receiverClient.CreateProcessor("reserve-inventory-test", new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 5,
            AutoCompleteMessages = false,
        });
        reserveProcessor.ProcessMessageAsync += async args =>
        {
            await Task.Delay(Random.Shared.Next(5, 20));
            await args.CompleteMessageAsync(args.Message);
            nonSessionCompleted.Add("reserve");
        };
        reserveProcessor.ProcessErrorAsync += args => { errors.Add(("reserve", args.Exception)); return Task.CompletedTask; };

        // Session processor (simulating ShipOrderConsumer on logistics-dispatch)
        var allSessionsDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sessionProcessor = receiverClient.CreateSessionProcessor("logistics-dispatch-test", new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 5,
            MaxConcurrentCallsPerSession = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(2),
            SessionIdleTimeout = TimeSpan.FromSeconds(5)
        });
        sessionProcessor.ProcessMessageAsync += async args =>
        {
            await Task.Delay(Random.Shared.Next(10, 50));
            await args.CompleteMessageAsync(args.Message);
            sessionCompleted.Add(args.Message.Body.ToString());
            if (sessionCompleted.Count >= orderCount)
                allSessionsDone.TrySetResult(true);
        };
        sessionProcessor.ProcessErrorAsync += args => { errors.Add(("session", args.Exception)); return Task.CompletedTask; };

        // Start all processors
        await pickProcessor.StartProcessingAsync();
        await reserveProcessor.StartProcessingAsync();
        await sessionProcessor.StartProcessingAsync();

        // Send messages to all queues concurrently (like the OrderApi does)
        var sender1 = senderClient.CreateSender("pick-order-test");
        var sender2 = senderClient.CreateSender("reserve-inventory-test");
        var sender3 = senderClient.CreateSender("logistics-dispatch-test");

        for (var i = 0; i < orderCount; i++)
        {
            var sessionId = $"warehouse-{i % sessionCount}";
            var tasks = new[]
            {
                sender1.SendMessageAsync(new ServiceBusMessage($"pick-{i}")),
                sender2.SendMessageAsync(new ServiceBusMessage($"reserve-{i}")),
                sender3.SendMessageAsync(new ServiceBusMessage($"ship-{i}") { SessionId = sessionId }),
            };
            await Task.WhenAll(tasks);
        }

        await sender1.CloseAsync();
        await sender2.CloseAsync();
        await sender3.CloseAsync();

        // Wait for session queue to drain (it's the bottleneck)
        var result = await Task.WhenAny(allSessionsDone.Task, Task.Delay(TimeSpan.FromSeconds(120)));

        await sessionProcessor.StopProcessingAsync();
        await pickProcessor.StopProcessingAsync();
        await reserveProcessor.StopProcessingAsync();

        var onDetachErrors = errors.Where(e => e.Error.Message.Contains("OnDetach")).ToList();
        var socketErrors = errors.Where(e =>
            e.Error is System.Net.Sockets.SocketException ||
            e.Error.InnerException is System.Net.Sockets.SocketException).ToList();

        Assert.True(onDetachErrors.Count == 0,
            $"OnDetach errors: {onDetachErrors.Count}. Sessions completed: {sessionCompleted.Count}/{orderCount}. First: {onDetachErrors.FirstOrDefault().Error?.Message}");
        Assert.True(socketErrors.Count == 0,
            $"Socket errors: {socketErrors.Count}. First: {socketErrors.FirstOrDefault().Error?.Message}");

        Assert.True(result == allSessionsDone.Task,
            $"Timed out. Sessions: {sessionCompleted.Count}/{orderCount}, NonSession: {nonSessionCompleted.Count}, Errors: {errors.Count}");
    }
}
