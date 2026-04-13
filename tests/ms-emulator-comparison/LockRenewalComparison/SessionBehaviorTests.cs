using Azure.Messaging.ServiceBus;
using Xunit.Abstractions;

namespace LockRenewalComparison;

/// <summary>
/// Session-specific behavior tests that exercise more than just lock renewal.
/// Covers: session state, AcceptNextSession, session isolation (two receivers,
/// two sessions), session-lock release on Dispose, message ordering within a
/// session, and message-level operations on session queues.
///
/// Queues required (defined in Config.json for the MS emulator, created by the
/// runner script for ours): "session-renewal-queue" (sessions, 10s lock).
/// </summary>
public class SessionBehaviorTests : IAsyncLifetime
{
    private const string SessionQueueName = "session-renewal-queue";

    private readonly ITestOutputHelper _output;
    private ServiceBusClient _client = null!;

    public SessionBehaviorTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public Task InitializeAsync()
    {
        var cs = Environment.GetEnvironmentVariable("SBE_CONNECTION_STRING")
            ?? throw new InvalidOperationException("SBE_CONNECTION_STRING is required.");
        _client = new ServiceBusClient(cs);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await _client.DisposeAsync();

    // ---------------------------------------------------------------- Accept mechanics

    [Fact]
    public async Task AcceptSession_PopulatesSessionLockedUntil()
    {
        // Regression guard for the bug we discovered: our emulator was sending
        // com.microsoft:locked-until-utc as a DateTime in the attach response,
        // but the SDK reads it as `long` ticks — so SessionLockedUntil was
        // always DateTime.MinValue. This test locks in the fix.
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("present")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);

        _output.WriteLine($"SessionLockedUntil: {receiver.SessionLockedUntil:O}");
        Assert.True(receiver.SessionLockedUntil > DateTimeOffset.UtcNow,
            $"SessionLockedUntil must be in the future after accept, got {receiver.SessionLockedUntil:O}");
    }

    [Fact]
    public async Task AcceptNextSession_ReturnsAvailableSession()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("any")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        // AcceptNextSessionAsync picks any available session with messages.
        // It should return THIS session since we just sent to it.
        await using var receiver = await _client.AcceptNextSessionAsync(
            SessionQueueName,
            new ServiceBusSessionReceiverOptions(),
            new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);

        Assert.NotNull(receiver);
        _output.WriteLine($"Got session: '{receiver.SessionId}'");
        Assert.False(string.IsNullOrEmpty(receiver.SessionId));

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        if (msg is not null) await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task AcceptSession_SecondReceiverIsRejectedWhileFirstHoldsLock()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("locked")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        await using var first = await _client.AcceptSessionAsync(SessionQueueName, sessionId);

        // Second client attempts to lock the same session — should fail because
        // the session is already locked. The SDK may surface this as a timeout
        // or a SessionCannotBeLocked exception; both are acceptable signals
        // that the lock is exclusive.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));
        var caught = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var second = await _client.AcceptSessionAsync(
                SessionQueueName, sessionId,
                new ServiceBusSessionReceiverOptions(),
                cts.Token);
        });
        _output.WriteLine($"Caught (expected): {caught.GetType().Name}: {caught.Message[..Math.Min(caught.Message.Length, 100)]}");
    }

    [Fact]
    public async Task MultipleSessions_ReceiversAreIsolated()
    {
        var s1 = $"s-{Guid.NewGuid():N}";
        var s2 = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("s1-msg")
        { MessageId = Guid.NewGuid().ToString(), SessionId = s1 });
        await sender.SendMessageAsync(new ServiceBusMessage("s2-msg")
        { MessageId = Guid.NewGuid().ToString(), SessionId = s2 });

        await using var r1 = await _client.AcceptSessionAsync(SessionQueueName, s1);
        await using var r2 = await _client.AcceptSessionAsync(SessionQueueName, s2);

        var m1 = await r1.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var m2 = await r2.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(m1);
        Assert.NotNull(m2);
        Assert.Equal(s1, m1.SessionId);
        Assert.Equal(s2, m2.SessionId);

        await r1.CompleteMessageAsync(m1);
        await r2.CompleteMessageAsync(m2);
    }

    // ---------------------------------------------------------------- Message ordering

    [Fact]
    public async Task SessionMessages_DeliveredInSendOrder()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);

        var sentIds = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            var id = $"ord-{i}-{Guid.NewGuid():N}";
            sentIds.Add(id);
            await sender.SendMessageAsync(new ServiceBusMessage($"m-{i}")
            { MessageId = id, SessionId = sessionId });
        }

        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
        var received = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg);
            received.Add(msg.MessageId);
            await receiver.CompleteMessageAsync(msg);
        }

        Assert.Equal(sentIds, received);
    }

    // ---------------------------------------------------------------- Session state

    [Fact]
    public async Task SessionState_RoundTripsBetweenReceivers()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("state-anchor")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        // First receiver sets state.
        var expected = new byte[] { 0xAB, 0xCD, 0xEF, 0x01, 0x02, 0x03 };
        {
            await using var r = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
            await r.SetSessionStateAsync(BinaryData.FromBytes(expected));
            // Consume the message so the session doesn't block indefinitely
            // on the next accept (emulators that require messages to be
            // drained will otherwise stall AcceptNextSession).
            var msg = await r.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
            if (msg is not null) await r.CompleteMessageAsync(msg);
        }

        // Second receiver reads the state — must see exactly what we wrote.
        // Send another message to keep the session alive for the second accept.
        await sender.SendMessageAsync(new ServiceBusMessage("state-anchor-2")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        await using var r2 = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
        var actual = await r2.GetSessionStateAsync();
        Assert.Equal(expected, actual.ToArray());

        // Cleanup
        var m = await r2.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        if (m is not null) await r2.CompleteMessageAsync(m);
    }

    [Fact]
    public async Task SessionState_NullByDefault()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("anchor")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
        var state = await receiver.GetSessionStateAsync();
        // Both null and empty are acceptable "no state set" representations.
        Assert.True(state is null || state.ToArray().Length == 0,
            $"Default session state should be null or empty, got {state?.ToArray().Length ?? -1} bytes");

        var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        if (m is not null) await receiver.CompleteMessageAsync(m);
    }

    [Fact]
    public async Task SessionState_CanBeClearedByPassingNull()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("anchor")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        await using var r = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
        await r.SetSessionStateAsync(BinaryData.FromBytes(new byte[] { 1, 2, 3 }));
        // Clear it
        await r.SetSessionStateAsync(null);

        var state = await r.GetSessionStateAsync();
        Assert.True(state is null || state.ToArray().Length == 0,
            $"State should be cleared, got {state?.ToArray().Length ?? -1} bytes");

        var m = await r.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        if (m is not null) await r.CompleteMessageAsync(m);
    }

    // ---------------------------------------------------------------- Message settlement

    [Fact]
    public async Task SessionMessage_Abandon_RedeliversInSameSession()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        var messageId = Guid.NewGuid().ToString();
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("retry-me")
        { MessageId = messageId, SessionId = sessionId });

        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);

        // First delivery → abandon.
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(first);
        Assert.Equal(messageId, first.MessageId);
        Assert.Equal(1, first.DeliveryCount);
        await receiver.AbandonMessageAsync(first);

        // Second delivery — same session, same message, DeliveryCount increased.
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(second);
        Assert.Equal(messageId, second.MessageId);
        Assert.True(second.DeliveryCount >= 2,
            $"Expected DeliveryCount >= 2 after Abandon, got {second.DeliveryCount}");
        await receiver.CompleteMessageAsync(second);
    }

    [Fact]
    public async Task SessionMessage_DeadLetter_MovesToDeadLetterQueue()
    {
        var sessionId = $"s-{Guid.NewGuid():N}";
        var messageId = Guid.NewGuid().ToString();
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("dead")
        { MessageId = messageId, SessionId = sessionId });

        await using (var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId))
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg);
            await receiver.DeadLetterMessageAsync(msg, "test-reason", "harness-dead-letter");
        }

        // Now peek the DLQ for this entity.
        var dlqPath = $"{SessionQueueName}/$deadletterqueue";
        await using var dlqReceiver = _client.CreateReceiver(dlqPath);
        ServiceBusReceivedMessage? dl = null;
        // DLQ may have prior messages; scan for our id.
        for (int i = 0; i < 10; i++)
        {
            var m = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
            if (m is null) break;
            if (m.MessageId == messageId) { dl = m; break; }
            await dlqReceiver.CompleteMessageAsync(m);
        }
        Assert.NotNull(dl);
        Assert.Equal("test-reason", dl.DeadLetterReason);
        await dlqReceiver.CompleteMessageAsync(dl);
    }

    [Fact]
    public async Task SessionMessage_RenewMessageLock_RejectedBySdk()
    {
        // The Azure SDK explicitly rejects per-message lock renewal on
        // session-enabled queues — session locks govern message lifetime.
        // Calling it must throw InvalidOperationException BEFORE reaching
        // the broker. Both emulators should exhibit identical behavior
        // because this is enforced entirely client-side.
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("sdk-enforced")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => receiver.RenewMessageLockAsync(msg));
        Assert.Contains("session", ex.Message, StringComparison.OrdinalIgnoreCase);
        _output.WriteLine($"Caught (as expected): {ex.Message}");

        await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task SessionLock_RenewExtendsExpiryByApproximatelyLockDuration()
    {
        // The queue is configured with LockDuration=10s in Config.json (MS) / the
        // runner script (ours). A successful renewal should push the expiry out
        // by roughly that amount — we allow a generous tolerance to absorb clock
        // skew between client and server.
        var sessionId = $"s-{Guid.NewGuid():N}";
        await using var sender = _client.CreateSender(SessionQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage("renewal-size")
        { MessageId = Guid.NewGuid().ToString(), SessionId = sessionId });

        await using var receiver = await _client.AcceptSessionAsync(SessionQueueName, sessionId);
        await Task.Delay(500);

        await receiver.RenewSessionLockAsync();
        var until = receiver.SessionLockedUntil;
        var delta = until - DateTimeOffset.UtcNow;
        _output.WriteLine($"Post-renew delta from now: {delta.TotalSeconds:F2}s");
        // 10s lock, allow [5s, 30s] as a very generous window.
        Assert.InRange(delta.TotalSeconds, 5.0, 30.0);

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        if (msg is not null) await receiver.CompleteMessageAsync(msg);
    }
}
