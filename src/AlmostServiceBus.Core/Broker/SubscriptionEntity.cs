using System.Collections.Concurrent;
using AlmostServiceBus.Core.Broker.Sql;

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

    public string? ForwardDeadLetteredMessagesTo
    {
        get => Queue.ForwardDeadLetteredMessagesTo;
        set => Queue.ForwardDeadLetteredMessagesTo = value;
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

    public TimeSpan DefaultMessageTimeToLive
    {
        get => Queue.DefaultMessageTimeToLive;
        set => Queue.DefaultMessageTimeToLive = value;
    }

    public string? UserMetadata { get; set; }

    // --- Rule management ---

    /// <exception cref="SqlSyntaxException">The rule's SQL filter or action is invalid, as Azure would reject it.</exception>
    public void AddOrUpdateRule(RuleEntity rule)
    {
        rule.Validate();
        _rules[rule.Name] = rule;
    }

    /// <summary>
    /// Adds the rule unless one with that name exists (Azure answers a create of an existing rule with 409).
    /// Atomic, so of concurrent creates of one name exactly one wins.
    /// </summary>
    /// <exception cref="SqlSyntaxException">The rule's SQL filter or action is invalid, as Azure would reject it.</exception>
    public bool TryAddRule(RuleEntity rule)
    {
        rule.Validate();
        return _rules.TryAdd(rule.Name, rule);
    }

    public RuleEntity? GetRule(string name) =>
        _rules.TryGetValue(name, out var rule) ? rule : null;

    public IReadOnlyCollection<RuleEntity> GetRules() =>
        _rules.Values.ToList();

    public bool RemoveRule(string name) =>
        _rules.TryRemove(name, out _);

    // --- Message delivery ---

    private const string FilteringEngine = "FilteringEngine";

    /// <summary>
    /// Delivers the copies Azure would: one per matching rule with an action (that rule's action applied and its
    /// name in <c>RuleName</c>), plus one unmodified copy if any matching rule has no action. A filter or action
    /// that fails dead-letters that rule's copy (<c>FilterException</c> / <c>RuleActionException</c>), or drops it
    /// when <see cref="DeadLetteringOnFilterEvaluationExceptions"/> is off; the other rules are unaffected.
    /// When <see cref="ForwardTo"/> is set the subscription's queue passes each copy on.
    /// </summary>
    public void DeliverMessage(BrokeredMessage message)
    {
        Queue.AssignSequenceNumber(message);
        var plainMatch = false;
        var actionCopies = new List<BrokeredMessage>();
        foreach (var rule in _rules.Values.OrderBy(r => r.Name, RuleKeyComparer))
        {
            bool matched;
            try
            {
                matched = rule.Matches(message);
            }
            catch (SqlEvaluationException ex)
            {
                if (DeadLetteringOnFilterEvaluationExceptions)
                    Queue.DeadLetterOnArrival(message.Clone(), "FilterException", ex.Message, FilteringEngine);
                continue;
            }

            if (!matched)
                continue;

            if (!rule.HasAction)
            {
                plainMatch = true;
                continue;
            }

            // Clone() drops the sequence number; the action still sees the message's own (sys.SequenceNumber).
            var copy = message.Clone();
            copy.SequenceNumber = message.SequenceNumber;
            try
            {
                rule.ApplyAction(copy);
            }
            catch (SqlEvaluationException ex)
            {
                if (DeadLetteringOnFilterEvaluationExceptions)
                {
                    copy.SequenceNumber = 0; // the dead-letter queue numbers its own messages
                    Queue.DeadLetterOnArrival(copy, "RuleActionException", ex.Message, FilteringEngine);
                }
                continue;
            }

            actionCopies.Add(copy);
        }

        // Deliver in sequence order: the first copy keeps the number already assigned, the rest are numbered
        // as they are enqueued, so a receiver never sees a later number before an earlier one.
        var deliveries = plainMatch ? [message, .. actionCopies] : actionCopies;
        for (var i = 1; i < deliveries.Count; i++)
            deliveries[i].SequenceNumber = 0;
        foreach (var delivery in deliveries)
            Enqueue(delivery);
    }

    private void Enqueue(BrokeredMessage message)
    {
        // A direct send to a session queue is refused, but a topic accepts the message and only
        // the session subscription dead-letters its copy; the other subscriptions still get theirs.
        if (RequiresSession && string.IsNullOrEmpty(message.SessionId))
        {
            Queue.DeadLetterOnArrival(message, "Session id is null.",
                "Session enabled entity doesn't allow a message whose session identifier is null.");
            return;
        }

        Queue.Enqueue(message);
    }
}
