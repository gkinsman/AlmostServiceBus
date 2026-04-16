using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;
using Xunit.Abstractions;

namespace AlmostServiceBus.Conformance.Tests;

/// <summary>
/// Reproduces the Wolverine will_requeue_and_increment_attempts failure pattern.
///
/// Wolverine's batched requeue does NOT use AMQP abandon. Instead it:
///   1. Re-publishes a brand new ServiceBusMessage back to the topic
///   2. Leaves the original message locked (eventually abandoned by processor)
///
/// This creates a race between the abandoned original and the re-published copy.
/// These tests verify delivery behavior for both emulator and real ASB.
/// </summary>
public abstract class TopicRequeueReproTestBase : IAsyncLifetime
{
    protected readonly ITestOutputHelper Output;
    protected ServiceBusClient Client = null!;
    protected ServiceBusAdministrationClient Admin = null!;
    protected string? SkipReason;

    private readonly List<string> _createdTopics = [];

    protected TopicRequeueReproTestBase(ITestOutputHelper output) => Output = output;

    protected abstract Task SetupClientsAsync();
    public abstract Task DisposeAsync();

    public async Task InitializeAsync() => await SetupClientsAsync();

    protected void ThrowIfSkipped()
    {
        if (SkipReason is not null)
            throw Xunit.Sdk.SkipException.ForSkip(SkipReason);
    }

    protected async Task<string> CreateTopicAsync()
    {
        var name = $"repro-{Guid.NewGuid():N}";
        await Admin.CreateTopicAsync(name);
        _createdTopics.Add(name);
        return name;
    }

    protected async Task CleanupTopicsAsync()
    {
        foreach (var t in _createdTopics)
            try { await Admin.DeleteTopicAsync(t); } catch { }
    }

    /// <summary>
    /// Wolverine inline requeue: receive → complete → re-publish to topic.
    /// Tests that 3 deliveries happen reliably.
    /// </summary>
    [Fact]
    public async Task CompleteAndRepublish_ViaTopicSubscription_DeliversThreeTimes()
    {
        ThrowIfSkipped();
        var topicName = await CreateTopicAsync();
        var subName = "test-sub";
        await Admin.CreateSubscriptionAsync(topicName, subName);

        var sw = Stopwatch.StartNew();
        void Log(string msg) => Output.WriteLine($"[{sw.ElapsedMilliseconds,6}ms] {msg}");

        var deliveries = new ConcurrentBag<int>();
        var succeeded = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempt = 0;

        await using var processor = Client.CreateProcessor(topicName, subName, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            AutoCompleteMessages = false,
            PrefetchCount = 0,
        });

        await using var sender = Client.CreateSender(topicName);

        processor.ProcessMessageAsync += async args =>
        {
            var currentAttempt = Interlocked.Increment(ref attempt);
            deliveries.Add(currentAttempt);
            Log($"RECEIVED attempt={currentAttempt} messageId={args.Message.MessageId} deliveryCount={args.Message.DeliveryCount}");

            await args.CompleteMessageAsync(args.Message);
            Log($"COMPLETED attempt={currentAttempt}");

            if (currentAttempt < 3)
            {
                var retry = new ServiceBusMessage("requeue-test-payload") { MessageId = Guid.NewGuid().ToString() };
                await sender.SendMessageAsync(retry);
                Log($"REPUBLISHED attempt={currentAttempt} → newMessageId={retry.MessageId}");
            }
            else
            {
                Log($"SUCCESS on attempt={currentAttempt}");
                succeeded.TrySetResult(true);
            }
        };

        processor.ProcessErrorAsync += args =>
        {
            Log($"PROCESSOR ERROR: {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        await sender.SendMessageAsync(new ServiceBusMessage("requeue-test-payload") { MessageId = Guid.NewGuid().ToString() });
        Log("INITIAL SEND");

        var completed = await Task.WhenAny(succeeded.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Log($"RESULT: deliveries={deliveries.Count} attempts=[{string.Join(", ", deliveries.OrderBy(x => x))}]");
        Assert.True(completed == succeeded.Task,
            $"Expected 3 deliveries but got {deliveries.Count}: [{string.Join(", ", deliveries.OrderBy(x => x))}]");
    }

    /// <summary>
    /// Wolverine batched requeue: throw (causing abandon) + re-publish.
    /// The abandoned original and re-published copy race — which arrives first?
    /// Logs the messageId to show delivery ordering.
    /// </summary>
    [Fact]
    public async Task BatchedRequeue_AbandonAndRepublish_ViaProcessor_DeliversThreeTimes()
    {
        ThrowIfSkipped();
        var topicName = await CreateTopicAsync();
        var subName = "test-sub";
        await Admin.CreateSubscriptionAsync(new CreateSubscriptionOptions(topicName, subName)
        {
            LockDuration = TimeSpan.FromSeconds(5),
        });

        var sw = Stopwatch.StartNew();
        void Log(string msg) => Output.WriteLine($"[{sw.ElapsedMilliseconds,6}ms] {msg}");

        var attempt = 0;
        var succeeded = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateProcessor(topicName, subName, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            AutoCompleteMessages = false,
            PrefetchCount = 0,
        });

        await using var sender = Client.CreateSender(topicName);

        processor.ProcessMessageAsync += async args =>
        {
            var currentAttempt = Interlocked.Increment(ref attempt);
            Log($"RECEIVED attempt={currentAttempt} messageId={args.Message.MessageId} deliveryCount={args.Message.DeliveryCount}");

            if (currentAttempt < 3)
            {
                var retryId = Guid.NewGuid().ToString();
                await sender.SendMessageAsync(new ServiceBusMessage("locked-payload") { MessageId = retryId });
                Log($"REPUBLISHED attempt={currentAttempt} → newMessageId={retryId}");

                // Throw to make processor abandon original (Released disposition)
                throw new InvalidOperationException($"Simulated failure on attempt {currentAttempt}");
            }

            await args.CompleteMessageAsync(args.Message);
            Log($"SUCCESS on attempt={currentAttempt}");
            succeeded.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            if (args.Exception is not InvalidOperationException)
                Log($"PROCESSOR ERROR: {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        await sender.SendMessageAsync(new ServiceBusMessage("locked-payload") { MessageId = Guid.NewGuid().ToString() });
        Log("INITIAL SEND");

        var completed = await Task.WhenAny(succeeded.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Log($"RESULT: attempts={attempt}");
        Assert.True(completed == succeeded.Task,
            $"Expected success on attempt 3 but only got {attempt} attempts in 30s");
    }

    /// <summary>
    /// Sessions enforce FIFO by sequence number. The abandoned original (lower seqNo)
    /// should always be delivered before the re-published copy (higher seqNo).
    /// </summary>
    [Fact]
    public async Task BatchedRequeue_WithSession_ForceDeterministicOrder()
    {
        ThrowIfSkipped();
        var topicName = await CreateTopicAsync();
        var subName = "test-sub";
        var sessionId = "test-session";

        await Admin.CreateSubscriptionAsync(new CreateSubscriptionOptions(topicName, subName)
        {
            RequiresSession = true,
            LockDuration = TimeSpan.FromSeconds(30),
        });

        var sw = Stopwatch.StartNew();
        void Log(string msg) => Output.WriteLine($"[{sw.ElapsedMilliseconds,6}ms] {msg}");

        await using var sender = Client.CreateSender(topicName);

        var initialId = Guid.NewGuid().ToString();
        await sender.SendMessageAsync(new ServiceBusMessage("session-payload")
        {
            MessageId = initialId,
            SessionId = sessionId,
        });
        Log($"INITIAL SEND messageId={initialId}");

        await using var sessionReceiver = await Client.AcceptSessionAsync(topicName, subName, sessionId,
            new ServiceBusSessionReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                PrefetchCount = 0,
            });

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            var msg = await sessionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            Assert.NotNull(msg);
            Log($"RECEIVED attempt={attempt} messageId={msg.MessageId} seqNo={msg.SequenceNumber} deliveryCount={msg.DeliveryCount}");

            if (attempt < 3)
            {
                await sessionReceiver.AbandonMessageAsync(msg);
                Log($"ABANDONED original seqNo={msg.SequenceNumber}");

                var retryId = Guid.NewGuid().ToString();
                await sender.SendMessageAsync(new ServiceBusMessage("session-payload")
                {
                    MessageId = retryId,
                    SessionId = sessionId,
                });
                Log($"REPUBLISHED → newMessageId={retryId}");
            }
            else
            {
                await sessionReceiver.CompleteMessageAsync(msg);
                Log($"SUCCESS on attempt={attempt}");
            }
        }

        Log("All 3 attempts delivered — session enforced order");
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Concrete: Emulator
// ══════════════════════════════════════════════════════════════════════════════

public class EmulatorTopicRequeueReproTest : TopicRequeueReproTestBase
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public EmulatorTopicRequeueReproTest(ITestOutputHelper output) : base(output) { }

    protected override async Task SetupClientsAsync()
    {
        await _fixture.StartAsync();
        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";

        Client = new ServiceBusClient(cs, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) }
        });

        Admin = new ServiceBusAdministrationClient(cs);
    }

    public override async Task DisposeAsync()
    {
        await Client.DisposeAsync();
        await _fixture.DisposeAsync();
    }

    /// <summary>
    /// Emulator-only: stress test with 10 parallel workers x 10 iterations.
    /// </summary>
    [Fact]
    public async Task CompleteAndRepublish_ParallelStressTest()
    {
        ThrowIfSkipped();
        const int parallelWorkers = 10;
        const int iterationsPerWorker = 10;
        var failures = new ConcurrentBag<(int worker, int iteration, int deliveryCount)>();
        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, parallelWorkers).Select(async worker =>
        {
            for (var i = 0; i < iterationsPerWorker; i++)
            {
                var topicName = await CreateTopicAsync();
                await Admin.CreateSubscriptionAsync(topicName, "test-sub");

                var attempt = 0;
                var succeeded = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var deliveryCount = 0;

                await using var processor = Client.CreateProcessor(topicName, "test-sub", new ServiceBusProcessorOptions
                {
                    MaxConcurrentCalls = 1,
                    ReceiveMode = ServiceBusReceiveMode.PeekLock,
                    AutoCompleteMessages = false,
                    PrefetchCount = 0,
                });

                await using var sender = Client.CreateSender(topicName);

                processor.ProcessMessageAsync += async args =>
                {
                    var a = Interlocked.Increment(ref attempt);
                    Interlocked.Increment(ref deliveryCount);
                    await args.CompleteMessageAsync(args.Message);
                    if (a < 3)
                        await sender.SendMessageAsync(new ServiceBusMessage("p") { MessageId = Guid.NewGuid().ToString() });
                    else
                        succeeded.TrySetResult(true);
                };
                processor.ProcessErrorAsync += _ => Task.CompletedTask;

                await processor.StartProcessingAsync();
                await sender.SendMessageAsync(new ServiceBusMessage("p") { MessageId = Guid.NewGuid().ToString() });

                var completed = await Task.WhenAny(succeeded.Task, Task.Delay(TimeSpan.FromSeconds(15)));
                await processor.StopProcessingAsync();

                if (completed != succeeded.Task)
                {
                    failures.Add((worker, i, deliveryCount));
                    Output.WriteLine($"[FAIL] worker={worker} iter={i} deliveries={deliveryCount}");
                }
            }
        });

        await Task.WhenAll(tasks);

        var total = parallelWorkers * iterationsPerWorker;
        Output.WriteLine($"\nResults: {total - failures.Count}/{total} passed in {sw.ElapsedMilliseconds}ms");
        Assert.Empty(failures);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Concrete: Real Azure Service Bus
// ══════════════════════════════════════════════════════════════════════════════

public class RealAsbTopicRequeueReproTest : TopicRequeueReproTestBase
{
    private static readonly string? ConnectionString =
        Environment.GetEnvironmentVariable("ASB_CONNECTION_STRING");

    public RealAsbTopicRequeueReproTest(ITestOutputHelper output) : base(output)
    {
        SkipReason = string.IsNullOrEmpty(ConnectionString)
            ? "ASB_CONNECTION_STRING not set"
            : null;
    }

    protected override Task SetupClientsAsync()
    {
        if (string.IsNullOrEmpty(ConnectionString))
            return Task.CompletedTask;

        Client = new ServiceBusClient(ConnectionString, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 2, TryTimeout = TimeSpan.FromSeconds(10) }
        });

        Admin = new ServiceBusAdministrationClient(ConnectionString);
        return Task.CompletedTask;
    }

    public override async Task DisposeAsync()
    {
        await CleanupTopicsAsync();
        if (Client is not null)
            await Client.DisposeAsync();
    }
}
