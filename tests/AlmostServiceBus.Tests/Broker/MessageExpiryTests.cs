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
}
