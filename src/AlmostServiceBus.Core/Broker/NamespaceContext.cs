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
        var created = false;
        var result = _queues.GetOrAdd(name, n =>
        {
            var queue = new QueueEntity(n);
            if (_eventBus is not null)
                queue.SetEventBus(_eventBus, Name, n);
            created = true;
            return queue;
        });

        // Resolve any subscriptions that reference this queue as ForwardTo
        // but couldn't resolve it at creation time (race: subscription created before queue).
        if (created)
        {
            foreach (var topic in _topics.Values)
            {
                foreach (var sub in topic.GetSubscriptions())
                {
                    if (sub.ResolvedForwardToQueue is null
                        && string.Equals(sub.ForwardTo, name, StringComparison.OrdinalIgnoreCase))
                    {
                        sub.ResolvedForwardToQueue = result;
                    }
                }
            }
        }

        return result;
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
        return _topics.GetOrAdd(name, n => new TopicEntity(n));
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
    /// When <paramref name="forwardTo"/> is provided the subscription's
    /// <see cref="SubscriptionEntity.ForwardTo"/> and
    /// <see cref="SubscriptionEntity.ResolvedForwardToQueue"/> are wired up.
    /// </summary>
    public SubscriptionEntity CreateSubscription(string topicName, string subName, string? forwardTo = null)
    {
        var topic = _topics.GetOrAdd(topicName, n => new TopicEntity(n));
        var sub = topic.AddSubscription(subName);

        if (forwardTo is not null)
        {
            sub.ForwardTo = forwardTo;
            // Auto-create the target queue if it doesn't exist — real ASB requires
            // ForwardTo targets to exist, and the management plane ensures they do.
            sub.ResolvedForwardToQueue = CreateQueue(forwardTo);
        }

        return sub;
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
