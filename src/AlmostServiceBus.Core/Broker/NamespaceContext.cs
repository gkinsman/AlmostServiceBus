using System.Collections.Concurrent;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// Holds all entities (queues and topics) for a single Service Bus namespace.
/// All dictionaries are case-insensitive to match Azure Service Bus behaviour.
/// </summary>
public sealed class NamespaceContext
{
    private static readonly StringComparer KeyComparer = StringComparer.OrdinalIgnoreCase;

    private readonly ConcurrentDictionary<string, QueueEntity> _queues = new(KeyComparer);
    private readonly ConcurrentDictionary<string, TopicEntity> _topics = new(KeyComparer);
    private readonly MessageEventBus? _eventBus;
    private long _sequenceNumber;

    public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

    private long _lastActivityAtTicks = DateTimeOffset.UtcNow.UtcTicks;

    public DateTimeOffset LastActivityAt
    {
        get => new(Interlocked.Read(ref _lastActivityAtTicks), TimeSpan.Zero);
        private set => Interlocked.Exchange(ref _lastActivityAtTicks, value.UtcTicks);
    }

    public void Touch() => Interlocked.Exchange(ref _lastActivityAtTicks, DateTimeOffset.UtcNow.UtcTicks);

    public NamespaceContext(string name, MessageEventBus? eventBus = null)
    {
        Name = name;
        _eventBus = eventBus;
    }

    public string Name { get; }

    // ── Queue methods ────────────────────────────────────────────────────────

    public QueueEntity CreateQueue(string name)
    {
        Touch();
        return _queues.GetOrAdd(name, n =>
        {
            var queue = new QueueEntity(n);
            queue.SetNamespace(this);
            if (_eventBus is not null)
                queue.SetEventBus(_eventBus, Name, n);
            return queue;
        });
    }

    public QueueEntity? GetQueue(string name) =>
        _queues.GetValueOrDefault(name);

    public IReadOnlyCollection<QueueEntity> GetQueues() =>
        _queues.Values.ToList();

    public bool DeleteQueue(string name) =>
        _queues.TryRemove(name, out _);

    // ── Topic methods ────────────────────────────────────────────────────────

    public TopicEntity CreateTopic(string name)
    {
        Touch();
        return _topics.GetOrAdd(name, NewTopic);
    }

    /// <summary>
    /// Creates a topic wired to the dashboard event bus, so the queues of its subscriptions
    /// stream Enqueued / Completed / DeadLettered events like top-level queues do.
    /// </summary>
    private TopicEntity NewTopic(string name)
    {
        var topic = new TopicEntity(name);
        topic.SetNamespace(this);
        if (_eventBus is not null)
            topic.SetEventBus(_eventBus, Name);
        return topic;
    }

    public TopicEntity? GetTopic(string name) =>
        _topics.GetValueOrDefault(name);

    public IReadOnlyCollection<TopicEntity> GetTopics() =>
        _topics.Values.ToList();

    public bool DeleteTopic(string name) =>
        _topics.TryRemove(name, out _);

    // ── Subscription convenience ─────────────────────────────────────────────

    /// <summary>
    /// Ensures the topic exists, then adds (or returns) a subscription on it.
    /// When <paramref name="forwardTo"/> names neither a queue nor a topic, a queue of
    /// that name is created: real ASB refuses a ForwardTo whose target doesn't exist.
    /// </summary>
    public SubscriptionEntity CreateSubscription(string topicName, string subName, string? forwardTo = null)
    {
        var topic = _topics.GetOrAdd(topicName, NewTopic);
        var sub = topic.AddSubscription(subName);

        if (forwardTo is not null)
        {
            if (ResolveSendTarget(forwardTo) is (null, null))
                CreateQueue(forwardTo);
            sub.ForwardTo = forwardTo;
        }

        return sub;
    }

    // ── Auto-forwarding ──────────────────────────────────────────────────────

    // Real ASB dead-letters a message on the 5th hop, and on the source entity
    // (not the destination) whenever it can't be forwarded. The reasons are the ones a
    // real namespace sets; they differ from the codes in Microsoft's documentation.
    private const int MaxForwardHops = 4;

    internal void Forward(QueueEntity source, BrokeredMessage message)
    {
        var target = source.ForwardTo!;
        var (queue, topic) = ResolveSendTarget(target);

        if (queue is null && topic is null)
        {
            source.DeadLetterOnArrival(message,
                $"The transfer destination entity does not exist. Transfer Destination Entity Details:- Name: {target}");
            return;
        }

        if (message.ForwardHopCount >= MaxForwardHops)
        {
            source.DeadLetterOnArrival(message, "Maximum transfer hop count is exceeded.");
            return;
        }

        if (queue is { RequiresSession: true } && string.IsNullOrEmpty(message.SessionId))
        {
            source.DeadLetterOnArrival(message,
                "The SessionId was not set on a message, and it cannot be sent to the entity. Entities that have session support enabled can only receive messages that have the SessionId set to a valid value.");
            return;
        }

        message.ForwardHopCount++;
        if (queue is not null)
            queue.Enqueue(message);
        else
            topic!.Publish(message);
    }

    public SubscriptionEntity? GetSubscription(string topicName, string subName) =>
        GetTopic(topicName)?.GetSubscription(subName);

    // ── Address resolution for AMQP ──────────────────────────────────────────

    /// <summary>
    /// Resolves an AMQP address to a <see cref="QueueEntity"/>.
    /// Supports:
    /// <list type="bullet">
    ///   <item>Direct queue name: <c>myQueue</c></item>
    ///   <item>Dead letter queue: <c>myQueue/$DeadLetterQueue</c></item>
    ///   <item>Subscription path: <c>topicName/Subscriptions/subName</c></item>
    ///   <item>Subscription DLQ: <c>topicName/Subscriptions/subName/$DeadLetterQueue</c></item>
    /// </list>
    /// Returns <see langword="null"/> when no match is found.
    /// </summary>
    public QueueEntity? ResolveQueue(string address)
    {
        if (_queues.TryGetValue(address, out var queue))
            return queue;

        // Check for $DeadLetterQueue suffix on a direct queue
        const string dlqSuffix = "/$DeadLetterQueue";
        if (address.EndsWith(dlqSuffix, StringComparison.OrdinalIgnoreCase))
        {
            var parentName = address[..^dlqSuffix.Length];

            // Try as a direct queue DLQ
            if (_queues.TryGetValue(parentName, out var parentQueue))
                return parentQueue.DeadLetterQueue;

            // Try as a subscription DLQ: "topicName/Subscriptions/subName/$DeadLetterQueue"
            var subParts = parentName.Split('/');
            if (subParts.Length >= 3 &&
                subParts[^2].Equals("Subscriptions", StringComparison.OrdinalIgnoreCase))
            {
                var topicName = string.Join('/', subParts[..^2]);
                var subName = subParts[^1];
                var sub = GetSubscription(topicName, subName);
                return sub?.Queue.DeadLetterQueue;
            }
        }

        // Try subscription path: "topicName/Subscriptions/subName"
        var parts = address.Split('/');
        if (parts.Length >= 3 &&
            parts[^2].Equals("Subscriptions", StringComparison.OrdinalIgnoreCase))
        {
            var topicName = string.Join('/', parts[..^2]);
            var subName = parts[^1];
            return GetSubscription(topicName, subName)?.Queue;
        }

        return null;
    }

    /// <summary>
    /// Resolves an AMQP send target address to either a queue or a topic.
    /// </summary>
    public (QueueEntity? Queue, TopicEntity? Topic) ResolveSendTarget(string address)
    {
        if (_queues.TryGetValue(address, out var queue))
            return (queue, null);

        if (_topics.TryGetValue(address, out var topic))
            return (null, topic);

        return (null, null);
    }

    // ── Sequence numbers ─────────────────────────────────────────────────────

    public long NextSequenceNumber() =>
        Interlocked.Increment(ref _sequenceNumber);
}
