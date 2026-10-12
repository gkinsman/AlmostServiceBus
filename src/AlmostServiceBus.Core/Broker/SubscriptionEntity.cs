using System.Collections.Concurrent;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// Represents a topic subscription with its own message queue and a set of filter rules.
/// </summary>
public sealed class SubscriptionEntity
{
    private static readonly StringComparer RuleKeyComparer = StringComparer.OrdinalIgnoreCase;

    private readonly ConcurrentDictionary<string, RuleEntity> _rules =
        new(RuleKeyComparer);

    public SubscriptionEntity(string name, string topicName)
    {
        Name = name;
        TopicName = topicName;
        Queue = new QueueEntity($"{topicName}/Subscriptions/{name}");

        // Every subscription starts with a single "$Default" TrueFilter rule.
        var defaultRule = new RuleEntity { Name = "$Default", FilterType = FilterType.TrueFilter };
        _rules[defaultRule.Name] = defaultRule;
    }

    // --- Identity ---

    public string Name { get; }

    public string TopicName { get; }

    // --- Own message store ---

    public QueueEntity Queue { get; }

    // --- Configuration ---
    // These properties forward to the underlying Queue so that the
    // AMQP layer (which works directly with QueueEntity) picks them up.

    public string? ForwardTo
    {
        get => Queue.ForwardTo;
        set => Queue.ForwardTo = value;
    }

    public int MaxDeliveryCount
    {
        get => Queue.MaxDeliveryCount;
        set => Queue.MaxDeliveryCount = value;
    }

    public TimeSpan LockDuration
    {
        get => Queue.LockDuration;
        set => Queue.LockDuration = value;
    }

    public bool DeadLetteringOnMessageExpiration
    {
        get => Queue.DeadLetteringOnMessageExpiration;
        set => Queue.DeadLetteringOnMessageExpiration = value;
    }

    public bool DeadLetteringOnFilterEvaluationExceptions { get; set; } = true;

    public TimeSpan? AutoDeleteOnIdle { get; set; }

    public bool EnableBatchedOperations { get; set; }

    public bool RequiresSession
    {
        get => Queue.RequiresSession;
        set => Queue.RequiresSession = value;
    }

    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;

    public string? UserMetadata { get; set; }

    // --- Rule management ---

    public void AddOrUpdateRule(RuleEntity rule) =>
        _rules[rule.Name] = rule;

    public RuleEntity? GetRule(string name) =>
        _rules.TryGetValue(name, out var rule) ? rule : null;

    public IReadOnlyCollection<RuleEntity> GetRules() =>
        _rules.Values.ToList();

    public bool RemoveRule(string name) =>
        _rules.TryRemove(name, out _);

    // --- Message delivery ---

    /// <summary>
    /// Returns <see langword="true"/> if at least one rule matches the message.
    /// </summary>
    public bool ShouldDeliver(BrokeredMessage message) =>
        _rules.Values.Any(r => r.Matches(message));

    /// <summary>
    /// Enqueues the message if a rule matches. When <see cref="ForwardTo"/> is set the
    /// subscription's queue passes it on to that queue or topic instead of holding it.
    /// </summary>
    public void DeliverMessage(BrokeredMessage message)
    {
        if (!ShouldDeliver(message))
            return;

        Queue.Enqueue(message);
    }
}
