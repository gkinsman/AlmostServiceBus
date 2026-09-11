using System.Collections.Concurrent;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// Represents a Service Bus topic that fans published messages out to all registered subscriptions.
/// </summary>
public sealed class TopicEntity
{
    private static readonly StringComparer SubscriptionKeyComparer = StringComparer.OrdinalIgnoreCase;

    private readonly ConcurrentDictionary<string, SubscriptionEntity> _subscriptions =
        new(SubscriptionKeyComparer);

    private MessageEventBus? _eventBus;
    private string? _namespaceName;

    public TopicEntity(string name)
    {
        Name = name;
    }

    /// <summary>
    /// Connects this topic's subscription queues to the dashboard event stream. A subscription's
    /// own queue (used when it has no ForwardTo) publishes under the entity path
    /// <c>{topic}/subscriptions/{subscription}</c>, which is how the dashboard addresses it.
    /// Applies to subscriptions added later as well as any that already exist.
    /// </summary>
    public void SetEventBus(MessageEventBus bus, string namespaceName)
    {
        _eventBus = bus;
        _namespaceName = namespaceName;
        foreach (var sub in _subscriptions.Values)
            WireEvents(sub);
    }

    private void WireEvents(SubscriptionEntity sub)
    {
        if (_eventBus is not null)
            sub.Queue.SetEventBus(_eventBus, _namespaceName ?? "", $"{Name}/subscriptions/{sub.Name}");
    }

    // --- Configuration ---

    public string Name { get; }

    public long MaxSizeInMegabytes { get; set; } = 1024L;

    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;

    public bool EnablePartitioning { get; set; } = false;

    public bool EnableExpress { get; set; } = false;

    public bool EnableBatchedOperations { get; set; } = true;

    public bool EnableSubscriptionPartitioning { get; set; } = false;

    public bool SupportOrdering { get; set; } = false;

    public TimeSpan? AutoDeleteOnIdle { get; set; }

    public bool RequiresDuplicateDetection { get; set; } = false;

    public TimeSpan DuplicateDetectionHistoryTimeWindow { get; set; } = TimeSpan.FromMinutes(10);

    public string? UserMetadata { get; set; }

    // --- Subscription management ---

    /// <summary>
    /// Adds a new subscription with the given name, or returns the existing one if it already exists.
    /// </summary>
    public SubscriptionEntity AddSubscription(string name) =>
        _subscriptions.GetOrAdd(name, n =>
        {
            var sub = new SubscriptionEntity(n, Name);
            WireEvents(sub);
            return sub;
        });

    public SubscriptionEntity? GetSubscription(string name) =>
        _subscriptions.TryGetValue(name, out var sub) ? sub : null;

    public IReadOnlyCollection<SubscriptionEntity> GetSubscriptions() =>
        _subscriptions.Values.ToList();

    public bool RemoveSubscription(string name) =>
        _subscriptions.TryRemove(name, out _);

    // --- Publishing ---

    /// <summary>
    /// Publishes a message to all subscriptions, cloning the message once per subscription.
    /// </summary>
    public void Publish(BrokeredMessage message)
    {
        foreach (var subscription in _subscriptions.Values)
        {
            subscription.DeliverMessage(message.Clone());
        }
    }
}
