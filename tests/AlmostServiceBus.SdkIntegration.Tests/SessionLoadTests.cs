using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Reproduces the "OnDetach is not valid under state: Start" error observed
/// in the OrderFlowDemo's logistics-dispatch session queue under heavy load.
/// The pattern: many session messages across multiple session IDs, processed
/// by a ServiceBusSessionProcessor with concurrent sessions.
/// </summary>
public class SessionLoadTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    private ServiceBusClient CreateClient()
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
    /// Simulates the OrderFlowDemo's logistics-dispatch queue under Black Friday load:
    /// 200 messages across 20 session IDs (warehouses), processed by a session processor
    /// with 5 concurrent sessions.
    /// </summary>
    [Fact]
    public async Task SessionProcessor_HighLoad_NoOnDetachErrors()
    {
        var queueName = "session-load-test";
        var context = _fixture.GetDefaultNamespaceContext();
        var queue = context.CreateQueue(queueName);
        queue.RequiresSession = true;
        queue.LockDuration = TimeSpan.FromSeconds(30);

        const int sessionCount = 20;
        const int messagesPerSession = 10;
        const int totalMessages = sessionCount * messagesPerSession;
        var sessionIds = Enumerable.Range(0, sessionCount).Select(i => $"warehouse-{i}").ToArray();

        await using var client = CreateClient();

        // Send all messages
        var sender = client.CreateSender(queueName);
        for (var i = 0; i < totalMessages; i++)
        {
            var sessionId = sessionIds[i % sessionCount];
            await sender.SendMessageAsync(new ServiceBusMessage($"order-{i}")
            {
                SessionId = sessionId,
                Subject = "ShipOrder"
            });
        }
        await sender.CloseAsync();

        // Process with concurrent sessions
        var completed = new ConcurrentBag<string>();
        var errors = new ConcurrentBag<(string Source, Exception Error)>();
        var allDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = client.CreateSessionProcessor(queueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 5,
            MaxConcurrentCallsPerSession = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(2),
            SessionIdleTimeout = TimeSpan.FromSeconds(5)
        });

        processor.ProcessMessageAsync += async args =>
        {
            // Simulate some processing time (like the ShipOrderConsumer)
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

        // Wait for all messages to be processed (generous timeout)
        var result = await Task.WhenAny(allDone.Task, Task.Delay(TimeSpan.FromSeconds(60)));

        await processor.StopProcessingAsync();

        // Report errors
        var onDetachErrors = errors.Where(e => e.Error.Message.Contains("OnDetach")).ToList();
        var socketErrors = errors.Where(e => e.Error is System.Net.Sockets.SocketException).ToList();
        var otherErrors = errors.Except(onDetachErrors).Except(socketErrors).ToList();

        Assert.True(onDetachErrors.Count == 0,
            $"Got {onDetachErrors.Count} OnDetach errors. First: {onDetachErrors.FirstOrDefault().Error?.Message}");
        Assert.True(socketErrors.Count == 0,
            $"Got {socketErrors.Count} socket errors. First: {socketErrors.FirstOrDefault().Error?.Message}");

        Assert.True(result == allDone.Task,
            $"Timed out. Completed {completed.Count}/{totalMessages}. Errors: {errors.Count} (OnDetach={onDetachErrors.Count}, Socket={socketErrors.Count}, Other={otherErrors.Count})");
    }

    /// <summary>
    /// Extreme load: 1000 messages across 50 sessions with 10 concurrent sessions.
    /// Simulates sustained Black Friday throughput.
    /// </summary>
    [Fact]
    public async Task SessionProcessor_BlackFridayLoad_NoErrors()
    {
        var queueName = "session-blackfriday";
        var context = _fixture.GetDefaultNamespaceContext();
        var queue = context.CreateQueue(queueName);
        queue.RequiresSession = true;
        queue.LockDuration = TimeSpan.FromSeconds(30);

        const int sessionCount = 50;
        const int messagesPerSession = 20;
        const int totalMessages = sessionCount * messagesPerSession;
        var sessionIds = Enumerable.Range(0, sessionCount).Select(i => $"warehouse-{i}").ToArray();

        await using var client = CreateClient();

        // Blast messages in parallel batches
        var sender = client.CreateSender(queueName);
        var sendTasks = new List<Task>();
        for (var i = 0; i < totalMessages; i++)
        {
            var sessionId = sessionIds[i % sessionCount];
            sendTasks.Add(sender.SendMessageAsync(new ServiceBusMessage($"order-{i}")
            {
                SessionId = sessionId,
                Subject = "ShipOrder"
            }));

            // Send in batches of 50
            if (sendTasks.Count >= 50)
            {
                await Task.WhenAll(sendTasks);
                sendTasks.Clear();
            }
        }
        if (sendTasks.Count > 0)
            await Task.WhenAll(sendTasks);
        await sender.CloseAsync();

        var completed = new ConcurrentBag<string>();
        var errors = new ConcurrentBag<(string Source, Exception Error)>();
        var allDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = client.CreateSessionProcessor(queueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 10,
            MaxConcurrentCallsPerSession = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(2),
            SessionIdleTimeout = TimeSpan.FromSeconds(3)
        });

        processor.ProcessMessageAsync += async args =>
        {
            await Task.Delay(Random.Shared.Next(5, 30));
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

        var result = await Task.WhenAny(allDone.Task, Task.Delay(TimeSpan.FromSeconds(180)));

        await processor.StopProcessingAsync();

        var onDetachErrors = errors.Where(e => e.Error.Message.Contains("OnDetach")).ToList();
        var socketErrors = errors.Where(e => e.Error is System.Net.Sockets.SocketException).ToList();

        Assert.True(onDetachErrors.Count == 0,
            $"Got {onDetachErrors.Count} OnDetach errors. First: {onDetachErrors.FirstOrDefault().Error?.Message}");
        Assert.True(socketErrors.Count == 0,
            $"Got {socketErrors.Count} socket errors. First: {socketErrors.FirstOrDefault().Error?.Message}");

        Assert.True(result == allDone.Task,
            $"Timed out. Completed {completed.Count}/{totalMessages}. Errors: {errors.Count}");
    }

    /// <summary>
    /// Same as above but with rapid session cycling — more sessions than MaxConcurrentSessions
    /// forces frequent session accept/release cycles.
    /// </summary>
    [Fact]
    public async Task SessionProcessor_RapidSessionCycling_NoErrors()
    {
        var queueName = "session-cycle-test";
        var context = _fixture.GetDefaultNamespaceContext();
        var queue = context.CreateQueue(queueName);
        queue.RequiresSession = true;
        queue.LockDuration = TimeSpan.FromSeconds(10);

        // 50 sessions with 2 messages each, processed by only 2 concurrent sessions
        // This forces rapid accept/release/re-accept cycling
        const int sessionCount = 50;
        const int messagesPerSession = 2;
        const int totalMessages = sessionCount * messagesPerSession;

        await using var client = CreateClient();

        var sender = client.CreateSender(queueName);
        for (var i = 0; i < totalMessages; i++)
        {
            var sessionId = $"session-{i % sessionCount}";
            await sender.SendMessageAsync(new ServiceBusMessage($"msg-{i}")
            {
                SessionId = sessionId
            });
        }
        await sender.CloseAsync();

        var completed = new ConcurrentBag<string>();
        var errors = new ConcurrentBag<(string Source, Exception Error)>();
        var allDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = client.CreateSessionProcessor(queueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 2,
            MaxConcurrentCallsPerSession = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(1),
            SessionIdleTimeout = TimeSpan.FromSeconds(2)
        });

        processor.ProcessMessageAsync += async args =>
        {
            await Task.Delay(Random.Shared.Next(5, 20));
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

        var result = await Task.WhenAny(allDone.Task, Task.Delay(TimeSpan.FromSeconds(120)));

        await processor.StopProcessingAsync();

        var onDetachErrors = errors.Where(e => e.Error.Message.Contains("OnDetach")).ToList();

        Assert.True(onDetachErrors.Count == 0,
            $"Got {onDetachErrors.Count} OnDetach errors. First: {onDetachErrors.FirstOrDefault().Error?.Message}");

        Assert.True(result == allDone.Task,
            $"Timed out. Completed {completed.Count}/{totalMessages}. Errors: {errors.Count}");
    }
}
