using Azure.Messaging.ServiceBus;
using Xunit.Abstractions;

namespace LockRenewalComparison;

/// <summary>
/// Lock-renewal behavior tests that run against BOTH Microsoft's official
/// Azure Service Bus emulator and our AlmostServiceBus emulator. The connection
/// string comes from the SBE_CONNECTION_STRING environment variable. A
/// test run produces a "result.txt" with pass/fail for each scenario — running
/// against both emulators and diff'ing the files reveals where we diverge.
///
/// The focus is on correctness of:
///  - Message-lock renewal (RenewMessageLockAsync)
///  - Session-lock renewal (RenewSessionLockAsync)
///  - Error modes: MessageLockLost, SessionLockLost, MessageNotFound
///
/// Queues expected to exist (see Config.json for MS emulator, or auto-created
/// for ours): "lock-renewal-queue" (no sessions, 10s lock) and
/// "session-renewal-queue" (sessions, 10s lock).
/// </summary>
public class LockRenewalTests : IAsyncLifetime
{
    private const string QueueName = "lock-renewal-queue";
    private const string SessionQueueName = "session-renewal-queue";

    private readonly ITestOutputHelper _output;
    private ServiceBusClient _client = null!;

    public LockRenewalTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public Task InitializeAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable("SBE_CONNECTION_STRING")
            ?? throw new InvalidOperationException(
                "SBE_CONNECTION_STRING environment variable is required. " +
                "For MS emulator: Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true " +
                "For ours: Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator");

        _client = new ServiceBusClient(connectionString);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
    }

    // ---------------------------------------------------------------- Message lock tests

    [Fact]
    public async Task MessageLock_RenewExtendsExpiry()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock });

        await sender.SendMessageAsync(new ServiceBusMessage("renew-me") { MessageId = Guid.NewGuid().ToString() });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(msg);

        var originalExpiry = msg.LockedUntil;
        _output.WriteLine($"Original expiry: {originalExpiry:O}");

        await receiver.RenewMessageLockAsync(msg);

        _output.WriteLine($"New expiry:      {msg.LockedUntil:O}");
        Assert.True(msg.LockedUntil > originalExpiry,
            $"LockedUntil should advance. Before: {originalExpiry:O}, After: {msg.LockedUntil:O}");

        await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task MessageLock_RenewAfterComplete_ThrowsMessageLockLost()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("complete-then-renew")
        { MessageId = Guid.NewGuid().ToString() });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(msg);
        await receiver.CompleteMessageAsync(msg);

        // Now try to renew — must fail with MessageLockLost.
        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            () => receiver.RenewMessageLockAsync(msg));
        _output.WriteLine($"Reason: {ex.Reason}");
        Assert.Equal(ServiceBusFailureReason.MessageLockLost, ex.Reason);
    }

    [Fact]
    public async Task MessageLock_RenewAfterAbandon_ThrowsMessageLockLost()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("abandon-then-renew")
        { MessageId = Guid.NewGuid().ToString() });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(msg);
        await receiver.AbandonMessageAsync(msg);

        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            () => receiver.RenewMessageLockAsync(msg));
        _output.WriteLine($"Reason: {ex.Reason}");
        Assert.Equal(ServiceBusFailureReason.MessageLockLost, ex.Reason);

        // Drain the re-delivered message to keep queue clean.
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        if (redelivered is not null)
            await receiver.CompleteMessageAsync(redelivered);
    }

    [Fact]
    public async Task MessageLock_RenewAfterNaturalExpiry_ThrowsMessageLockLost()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("expire-then-renew")
        { MessageId = Guid.NewGuid().ToString() });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(msg);

        // Wait past the 10-second LockDuration so the broker expires and re-enqueues
        // the message. The broker should assign a new lock token to the re-delivered
        // copy, making the original one we hold invalid.
        _output.WriteLine($"Sleeping past lock expiry {msg.LockedUntil:O}...");
        var waitSpan = msg.LockedUntil - DateTimeOffset.UtcNow + TimeSpan.FromSeconds(12);
        if (waitSpan > TimeSpan.Zero)
            await Task.Delay(waitSpan);

        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            () => receiver.RenewMessageLockAsync(msg));
        _output.WriteLine($"Reason: {ex.Reason}");
        Assert.Equal(ServiceBusFailureReason.MessageLockLost, ex.Reason);

        // Drain the re-delivered message.
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(15));
        if (redelivered is not null)
            await receiver.CompleteMessageAsync(redelivered);
    }

    [Fact]
    public async Task MessageLock_RenewMultipleTimes_Succeeds()
    {
        await using var sender = _client.CreateSender(QueueName);
        await using var receiver = _client.CreateReceiver(QueueName);

        await sender.SendMessageAsync(new ServiceBusMessage("renew-3x")
        { MessageId = Guid.NewGuid().ToString() });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(msg);

        for (int i = 0; i < 3; i++)
        {
            var before = msg.LockedUntil;
            await Task.Delay(500);
            await receiver.RenewMessageLockAsync(msg);
            _output.WriteLine($"Renewal {i + 1}: {before:O} -> {msg.LockedUntil:O}");
            Assert.True(msg.LockedUntil > before);
        }

        await receiver.CompleteMessageAsync(msg);
    }

    // ---------------------------------------------------------------- Session lock tests

    [Fact]
    public async Task SessionLock_RenewExtendsExpiry()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("in-session")
        {
            MessageId = Guid.NewGuid().ToString(),
            SessionId = sessionId
        });

        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
        var originalExpiry = receiver.SessionLockedUntil;
        _output.WriteLine($"Original session expiry: {originalExpiry:O}");

        await Task.Delay(500);
        await receiver.RenewSessionLockAsync();

        _output.WriteLine($"New session expiry:      {receiver.SessionLockedUntil:O}");
        Assert.True(receiver.SessionLockedUntil > originalExpiry,
            $"SessionLockedUntil should advance. Before: {originalExpiry:O}, After: {receiver.SessionLockedUntil:O}");

        // Consume the test message so the next test run starts clean.
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        if (msg is not null) await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task SessionLock_RenewAfterNaturalExpiry_ThrowsSessionLockLost()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("session-expires")
        {
            MessageId = Guid.NewGuid().ToString(),
            SessionId = sessionId
        });

        var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
        _output.WriteLine($"Session expires (from SDK): {receiver.SessionLockedUntil:O}");

        // Wait for LockDuration (10s configured on the queue) plus grace. Don't rely
        // on receiver.SessionLockedUntil because the Azure SDK may or may not populate
        // it from the attach response — both emulators should agree on behavior here
        // regardless of whether the SDK sees the initial expiry or not.
        await Task.Delay(TimeSpan.FromSeconds(12));

        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            () => receiver.RenewSessionLockAsync());
        _output.WriteLine($"Reason: {ex.Reason}");
        Assert.Equal(ServiceBusFailureReason.SessionLockLost, ex.Reason);

        await receiver.DisposeAsync();
    }

    [Fact]
    public async Task SessionLock_RenewMultipleTimes_Succeeds()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("multi-renew")
        {
            MessageId = Guid.NewGuid().ToString(),
            SessionId = sessionId
        });

        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);

        for (int i = 0; i < 3; i++)
        {
            var before = receiver.SessionLockedUntil;
            await Task.Delay(500);
            await receiver.RenewSessionLockAsync();
            _output.WriteLine($"Session renewal {i + 1}: {before:O} -> {receiver.SessionLockedUntil:O}");
            Assert.True(receiver.SessionLockedUntil > before);
        }

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        if (msg is not null) await receiver.CompleteMessageAsync(msg);
    }

    // ---------------------------------------------------------------- High-throughput simulation

    [Fact]
    public async Task MessageLock_AutoRenewalSurvivesSlowConsumer()
    {
        // This test exercises the exact scenario that causes R-DUPE cascades under
        // Black Friday load: a consumer that holds a message longer than LockDuration
        // and relies on the SDK's built-in lock auto-renewal to keep it alive.
        await using var sender = _client.CreateSender(QueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("slow-processing")
        { MessageId = Guid.NewGuid().ToString() });

        var processedOk = false;
        await using var processor = _client.CreateProcessor(QueueName, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(2),
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
        });

        processor.ProcessMessageAsync += async args =>
        {
            _output.WriteLine($"Got message, sleeping 25s (lock is 10s, auto-renewal must kick in)...");
            await Task.Delay(TimeSpan.FromSeconds(25), args.CancellationToken);
            try
            {
                await args.CompleteMessageAsync(args.Message, args.CancellationToken);
                processedOk = true;
                _output.WriteLine("Completed successfully — auto-renewal worked.");
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Complete failed: {ex.GetType().Name}: {ex.Message}");
            }
        };
        processor.ProcessErrorAsync += args =>
        {
            _output.WriteLine($"ERR: {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        // Give it long enough for the 25s sleep + complete to finish.
        for (int i = 0; i < 40 && !processedOk; i++)
            await Task.Delay(TimeSpan.FromSeconds(1));
        await processor.StopProcessingAsync();

        Assert.True(processedOk, "SDK auto-renewal must keep the lock alive across slow processing.");
    }
}
