using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Time.Testing;

namespace AlmostServiceBus.Tests.Broker;

public class MessageExpiryTests
{
    private const string ExpiredReason = "TTLExpiredException";
    private const string ExpiredDescription = "The message expired and was dead lettered.";

    private readonly FakeTimeProvider _time = new(DateTimeOffset.UtcNow);

    private BrokeredMessage Message(TimeSpan? ttl = null, string? sessionId = null, string id = "m") => new()
    {
        MessageId = id,
        Body = "x"u8.ToArray(),
        SessionId = sessionId,
        TimeToLive = ttl ?? TimeSpan.MaxValue,
        EnqueuedTimeUtc = _time.GetUtcNow(),
    };

    private QueueEntity Queue(TimeSpan? defaultTtl = null, bool deadLetterOnExpiry = false) => new("q")
    {
        TimeProvider = _time,
        DefaultMessageTimeToLive = defaultTtl ?? TimeSpan.MaxValue,
        DeadLetteringOnMessageExpiration = deadLetterOnExpiry,
    };

    // ── Effective TTL at enqueue ─────────────────────────────────────────────

    [Fact]
    public void Enqueue_CapsTtlAtEntityDefault()
    {
        var queue = Queue(defaultTtl: TimeSpan.FromSeconds(30));
        var message = Message(TimeSpan.FromHours(1));

        queue.Enqueue(message);

        Assert.Equal(TimeSpan.FromHours(1), message.TimeToLive);
        Assert.Equal(TimeSpan.FromSeconds(30), message.EffectiveTimeToLive);
        Assert.Equal(message.EnqueuedTimeUtc.AddSeconds(30), message.ExpiresAtUtc);
    }

    [Fact]
    public void Enqueue_ShorterTtlThanDefault_KeepsMessageTtl()
    {
        var queue = Queue(defaultTtl: TimeSpan.FromSeconds(30));
        var message = Message(TimeSpan.FromSeconds(10));

        queue.Enqueue(message);

        Assert.Equal(TimeSpan.FromSeconds(10), message.EffectiveTimeToLive);
        Assert.Equal(message.EnqueuedTimeUtc.AddSeconds(10), message.ExpiresAtUtc);
    }

    [Fact]
    public void Enqueue_NoTtl_UsesEntityDefault()
    {
        var queue = Queue(defaultTtl: TimeSpan.FromSeconds(30));
        var message = Message();

        queue.Enqueue(message);

        Assert.Equal(TimeSpan.FromSeconds(30), message.EffectiveTimeToLive);
    }

    [Fact]
    public void Enqueue_MaxTtlAndMaxDefault_ExpiresAtMaxValue()
    {
        var queue = Queue();
        var message = Message();

        queue.Enqueue(message);

        Assert.Equal(TimeSpan.MaxValue, message.EffectiveTimeToLive);
        Assert.Equal(DateTimeOffset.MaxValue, message.ExpiresAtUtc);
    }

    [Fact]
    public void Publish_AppliesTopicThenSubscriptionCap()
    {
        var ns = new NamespaceContext("ns");
        var topic = ns.CreateTopic("t");
        topic.DefaultMessageTimeToLive = TimeSpan.FromSeconds(40);
        var shortSub = ns.CreateSubscription("t", "short");
        shortSub.DefaultMessageTimeToLive = TimeSpan.FromSeconds(20);
        var longSub = ns.CreateSubscription("t", "long");
        longSub.DefaultMessageTimeToLive = TimeSpan.FromHours(1);

        topic.Publish(Message(TimeSpan.FromHours(2)));

        Assert.Equal(TimeSpan.FromSeconds(20), shortSub.Queue.TryDequeueImmediate()!.EffectiveTimeToLive);
        Assert.Equal(TimeSpan.FromSeconds(40), longSub.Queue.TryDequeueImmediate()!.EffectiveTimeToLive);
    }

    [Fact]
    public void ConvertToAmqpMessage_ReceiveUsesEffectiveTtl_PeekUsesSentTtl()
    {
        var queue = Queue(defaultTtl: TimeSpan.FromSeconds(30));
        var message = Message(TimeSpan.FromHours(1));
        queue.Enqueue(message);

        var received = ReceiverLinkEndpoint.ConvertToAmqpMessage(message);
        var peeked = ReceiverLinkEndpoint.ConvertToAmqpMessage(message, peek: true);

        Assert.Equal(30_000u, received.Header.Ttl);
        Assert.Equal(message.EnqueuedTimeUtc.AddSeconds(30).UtcDateTime, received.Properties.AbsoluteExpiryTime);
        Assert.Equal(3_600_000u, peeked.Header.Ttl);
        Assert.Equal(message.EnqueuedTimeUtc.AddHours(1).UtcDateTime, peeked.Properties.AbsoluteExpiryTime);
    }

    [Fact]
    public void ConvertToAmqpMessage_PeekWithoutSentTtl_HasNoTtl()
    {
        var queue = Queue(defaultTtl: TimeSpan.FromSeconds(30));
        var message = Message();
        queue.Enqueue(message);

        var peeked = ReceiverLinkEndpoint.ConvertToAmqpMessage(message, peek: true);

        Assert.Equal(new global::Amqp.Framing.Header().Ttl, peeked.Header.Ttl); // unset
        Assert.Equal(default, peeked.Properties.AbsoluteExpiryTime);
    }

    // ── Never deliver an expired message ─────────────────────────────────────

    [Fact]
    public void TryDequeueImmediate_SkipsExpired_DeadLettersWhenEnabled()
    {
        var queue = Queue(deadLetterOnExpiry: true);
        queue.Enqueue(Message(TimeSpan.FromSeconds(5), id: "short"));
        queue.Enqueue(Message(id: "forever"));
        _time.Advance(TimeSpan.FromSeconds(6));

        Assert.Equal("forever", queue.TryDequeueImmediate()?.MessageId);
        var dead = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.Equal("short", dead?.MessageId);
        Assert.Equal(ExpiredReason, dead!.DeadLetterReason);
        Assert.Equal(ExpiredDescription, dead.DeadLetterErrorDescription);
        Assert.Equal(0, queue.MessageCount);
    }

    [Fact]
    public void TryDequeueImmediate_SkipsExpired_DropsWhenDisabled()
    {
        var queue = Queue();
        queue.Enqueue(Message(TimeSpan.FromSeconds(5)));
        _time.Advance(TimeSpan.FromSeconds(5));

        Assert.Null(queue.TryDequeueImmediate());
        Assert.Equal(0, queue.DeadLetterQueue.MessageCount);
        Assert.Equal(0, queue.ActiveMessageCount);
        Assert.Empty(queue.PeekFromSequence(0, 10));
    }

    [Fact]
    public async Task DequeueAsync_SkipsExpired()
    {
        var queue = Queue();
        queue.Enqueue(Message(TimeSpan.FromSeconds(5), id: "short"));
        queue.Enqueue(Message(id: "forever"));
        _time.Advance(TimeSpan.FromSeconds(6));

        var message = await queue.DequeueAsync(new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);

        Assert.Equal("forever", message.MessageId);
    }

    [Fact]
    public void NotYetExpired_IsDelivered()
    {
        var queue = Queue();
        queue.Enqueue(Message(TimeSpan.FromSeconds(5)));
        _time.Advance(TimeSpan.FromSeconds(4));

        Assert.NotNull(queue.TryDequeueImmediate());
    }

    // ── Sweep ────────────────────────────────────────────────────────────────

    [Fact]
    public void ExpireMessages_SweepsWithoutReceiver()
    {
        var queue = Queue(deadLetterOnExpiry: true);
        queue.Enqueue(Message(TimeSpan.FromSeconds(5)));
        queue.Enqueue(Message(id: "forever"));
        _time.Advance(TimeSpan.FromSeconds(6));

        Assert.Equal(1, queue.ExpireMessages());

        Assert.Equal(1, queue.MessageCount);
        Assert.Equal(1, queue.DeadLetterQueue.MessageCount);
        Assert.Equal("forever", queue.TryDequeueImmediate()?.MessageId);
        // The swept message's stale channel entry is skipped, so a purge loop terminates.
        Assert.Null(queue.TryDequeueImmediate());
    }

    [Fact]
    public void ExpireMessages_LeavesLockedMessage_ThenExpiresAfterRelease()
    {
        var queue = Queue(deadLetterOnExpiry: true);
        queue.Enqueue(Message(TimeSpan.FromSeconds(5)));
        var locked = queue.TryDequeueImmediate()!;
        _time.Advance(TimeSpan.FromSeconds(6));

        Assert.Equal(0, queue.ExpireMessages());
        Assert.Equal(0, queue.DeadLetterQueue.MessageCount);

        queue.Release(locked.LockToken!);

        Assert.Equal(1, queue.ExpireMessages());
        Assert.Equal(1, queue.DeadLetterQueue.MessageCount);
    }

    [Fact]
    public async Task Abandon_AfterTtl_ExpiresInsteadOfRedelivering()
    {
        var queue = Queue(deadLetterOnExpiry: true);
        queue.Enqueue(Message(TimeSpan.FromSeconds(5)));
        var locked = queue.TryDequeueImmediate()!;
        _time.Advance(TimeSpan.FromSeconds(6));

        queue.Abandon(locked.LockToken!);

        // Abandon makes the message available again after a real one-second delay.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (queue.DeadLetterQueue.MessageCount == 0 && DateTime.UtcNow < deadline)
        {
            Assert.Null(queue.TryDequeueImmediate());
            await Task.Delay(50);
        }
        Assert.Equal(1, queue.DeadLetterQueue.MessageCount);
        Assert.Equal(0, queue.ActiveMessageCount);
    }

    [Fact]
    public void Complete_AfterTtl_WhileLocked_Succeeds()
    {
        var queue = Queue();
        queue.Enqueue(Message(TimeSpan.FromSeconds(5)));
        var locked = queue.TryDequeueImmediate()!;
        _time.Advance(TimeSpan.FromSeconds(6));

        queue.Complete(locked.LockToken!);

        Assert.Equal(1, queue.ConsumedCount);
    }

    [Fact]
    public void DeadLetterQueue_NeverExpires()
    {
        var queue = Queue();
        queue.Enqueue(Message(TimeSpan.FromSeconds(5)));
        var locked = queue.TryDequeueImmediate()!;
        queue.DeadLetter(locked.LockToken!, "reason", "description");
        _time.Advance(TimeSpan.FromDays(1));

        Assert.Equal(0, queue.DeadLetterQueue.ExpireMessages());
        Assert.NotNull(queue.DeadLetterQueue.TryDequeueImmediate());
    }

    [Fact]
    public void Expired_ForwardsToForwardDeadLetteredMessagesTo()
    {
        var ns = new NamespaceContext("ns", timeProvider: _time);
        var target = ns.CreateQueue("target");
        var source = ns.CreateQueue("source");
        source.DeadLetteringOnMessageExpiration = true;
        source.ForwardDeadLetteredMessagesTo = "target";
        source.Enqueue(Message(TimeSpan.FromSeconds(5)));
        _time.Advance(TimeSpan.FromSeconds(6));

        Assert.Equal(1, source.ExpireMessages());

        // Real Service Bus restarts the clock at the destination, so it is still deliverable there.
        var forwarded = target.TryDequeueImmediate();
        Assert.NotNull(forwarded);
        Assert.Equal(ExpiredReason, forwarded.DeadLetterReason);
        Assert.Equal(ExpiredDescription, forwarded.DeadLetterErrorDescription);
        Assert.Equal(0, source.DeadLetterQueue.MessageCount);
    }

    [Fact]
    public async Task ConcurrentSweepAndDequeue_EachMessageTakenOnce()
    {
        var queue = Queue(deadLetterOnExpiry: true);
        for (var i = 0; i < 2000; i++)
            queue.Enqueue(Message(i % 2 == 0 ? TimeSpan.FromSeconds(1) : null, id: i.ToString()));
        _time.Advance(TimeSpan.FromSeconds(2));

        var delivered = new System.Collections.Concurrent.ConcurrentBag<string>();
        var dequeuers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (queue.TryDequeueImmediate() is { } m)
                delivered.Add(m.MessageId);
        }));
        var sweepers = Enumerable.Range(0, 2).Select(_ => Task.Run(() => queue.ExpireMessages()));
        await Task.WhenAll(dequeuers.Concat(sweepers));

        Assert.Equal(1000, delivered.Count);
        Assert.Equal(1000, delivered.Distinct().Count());
        Assert.All(delivered, id => Assert.True(int.Parse(id) % 2 == 1));
        Assert.Equal(1000, queue.DeadLetterQueue.MessageCount);
        Assert.Equal(0, queue.MessageCount);
    }

    // ── Sessions ─────────────────────────────────────────────────────────────

    [Fact]
    public void TryDequeueFromSession_SkipsExpired()
    {
        var queue = Queue(deadLetterOnExpiry: true);
        queue.RequiresSession = true;
        queue.Enqueue(Message(TimeSpan.FromSeconds(5), sessionId: "s1", id: "short"));
        queue.Enqueue(Message(sessionId: "s1", id: "forever"));
        _time.Advance(TimeSpan.FromSeconds(6));

        var session = queue.Sessions!.TryAcceptSession("s1", "receiver")!;

        Assert.True(queue.TryDequeueFromSession(session, out var message));
        Assert.Equal("forever", message.MessageId);
        Assert.False(queue.TryDequeueFromSession(session, out _));
        Assert.Equal("short", queue.DeadLetterQueue.TryDequeueImmediate()?.MessageId);
    }

    [Fact]
    public void ExpireMessages_RemovesFromSession_SoNextAvailableSkipsIt()
    {
        var queue = Queue();
        queue.RequiresSession = true;
        queue.Enqueue(Message(TimeSpan.FromSeconds(5), sessionId: "expired"));
        queue.Enqueue(Message(sessionId: "live"));
        _time.Advance(TimeSpan.FromSeconds(6));

        Assert.Equal(1, queue.ExpireMessages());

        Assert.Equal("live", queue.Sessions!.TryAcceptSession(null, "receiver")?.SessionId);
        Assert.Null(queue.Sessions.TryAcceptSession(null, "other"));
        Assert.Equal(1, queue.MessageCount);
    }

    // ── Forwarding and scheduling clocks ─────────────────────────────────────

    [Fact]
    public void ForwardTo_RestartsClockAtDestination()
    {
        var ns = new NamespaceContext("ns", timeProvider: _time);
        var target = ns.CreateQueue("target");
        var source = ns.CreateQueue("source");
        source.ForwardTo = "target";
        var message = Message(TimeSpan.FromSeconds(5));
        _time.Advance(TimeSpan.FromSeconds(4));

        source.Enqueue(message);
        _time.Advance(TimeSpan.FromSeconds(3));

        Assert.NotNull(target.TryDequeueImmediate());
    }

    [Fact]
    public void ScheduledMessage_ClockStartsAtSend()
    {
        // Real Service Bus counts a scheduled message's time-to-live from when it was sent, so one
        // that outlives its TTL before activating goes straight to the dead-letter queue.
        var ns = new NamespaceContext("ns", timeProvider: _time);
        var queue = ns.CreateQueue("q");
        queue.DeadLetteringOnMessageExpiration = true;
        using var scheduler = new ScheduledMessageProcessor(ns);
        var message = Message(TimeSpan.FromSeconds(2));
        message.ScheduledEnqueueTimeUtc = _time.GetUtcNow().AddSeconds(3);
        scheduler.Schedule("q", message, ns);

        _time.Advance(TimeSpan.FromSeconds(2.5));
        scheduler.ProcessDueMessages();
        Assert.Equal(0, queue.MessageCount);

        _time.Advance(TimeSpan.FromSeconds(1));
        scheduler.ProcessDueMessages();

        Assert.Null(queue.TryDequeueImmediate());
        Assert.Equal(ExpiredReason, queue.DeadLetterQueue.TryDequeueImmediate()?.DeadLetterReason);
    }

    // ── Background processor ─────────────────────────────────────────────────

    [Fact]
    public void Processor_SweepsAllNamespacesQueuesAndSubscriptions()
    {
        var registry = new NamespaceRegistry(timeProvider: _time);
        var queue = registry.GetOrCreate("a").CreateQueue("q");
        var nsB = registry.GetOrCreate("b");
        var sub = nsB.CreateSubscription("t", "s");
        queue.Enqueue(Message(TimeSpan.FromSeconds(1)));
        nsB.GetTopic("t")!.Publish(Message(TimeSpan.FromSeconds(1)));
        _time.Advance(TimeSpan.FromSeconds(2));
        using var processor = new MessageExpiryProcessor(registry);

        Assert.Equal(2, processor.ProcessExpiredMessages());

        Assert.Equal(0, queue.MessageCount);
        Assert.Equal(0, sub.Queue.MessageCount);
    }

    [Fact]
    public void Processor_ContinuesAfterOneEntityThrows()
    {
        var registry = new NamespaceRegistry(timeProvider: _time);
        var ns = registry.GetOrCreate("a");
        ns.CreateQueue("broken").TimeProvider = new ThrowingTimeProvider();
        var healthy = ns.CreateQueue("healthy");
        healthy.Enqueue(Message(TimeSpan.FromSeconds(1)));
        _time.Advance(TimeSpan.FromSeconds(2));
        using var processor = new MessageExpiryProcessor(registry);

        Assert.Equal(1, processor.ProcessExpiredMessages());
        Assert.Equal(0, healthy.MessageCount);
    }

    private sealed class ThrowingTimeProvider : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => throw new InvalidOperationException("clock broken");
    }
}
