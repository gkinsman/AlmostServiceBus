namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// The <c>sys.</c> properties a filter can read and an action can set, mapped onto <see cref="BrokeredMessage"/>.
/// Names are case-insensitive. The readable set and the values match what Azure exposes when filters run.
/// </summary>
internal static class SqlSystemProperties
{
    private sealed record Property(Type Type, Func<BrokeredMessage, object?> Get, Action<BrokeredMessage, object?>? Set = null);

    private static readonly Dictionary<string, Property> Properties = new(StringComparer.OrdinalIgnoreCase)
    {
        ["MessageId"] = new(typeof(string), m => m.MessageId, (m, v) => m.MessageId = (string)v!),
        ["CorrelationId"] = new(typeof(string), m => m.CorrelationId, (m, v) => m.CorrelationId = (string?)v),
        ["To"] = new(typeof(string), m => m.To, (m, v) => m.To = (string?)v),
        ["ReplyTo"] = new(typeof(string), m => m.ReplyTo, (m, v) => m.ReplyTo = (string?)v),
        ["Label"] = new(typeof(string), m => m.Subject, (m, v) => m.Subject = (string?)v),
        ["SessionId"] = new(typeof(string), m => m.SessionId, (m, v) =>
        {
            m.SessionId = (string?)v;
            m.PartitionKey = (string?)v;
        }),
        ["ReplyToSessionId"] = new(typeof(string), m => m.ReplyToSessionId, (m, v) => m.ReplyToSessionId = (string?)v),
        ["ContentType"] = new(typeof(string), m => m.ContentType, (m, v) => m.ContentType = (string?)v),
        ["PartitionKey"] = new(typeof(string), m => m.PartitionKey, (m, v) => m.PartitionKey = (string?)v),
        ["TimeToLive"] = new(typeof(TimeSpan), m => m.TimeToLive, (m, v) => m.TimeToLive = (TimeSpan)v!),
        ["ScheduledEnqueueTimeUtc"] = new(typeof(DateTime),
            m => m.ScheduledEnqueueTimeUtc?.UtcDateTime ?? DateTime.MinValue,
            (m, v) => m.ScheduledEnqueueTimeUtc = new DateTimeOffset(DateTime.SpecifyKind((DateTime)v!, DateTimeKind.Utc))),
        ["ViaPartitionKey"] = new(typeof(string), _ => null),
        ["SequenceNumber"] = new(typeof(long), m => m.SequenceNumber),
        ["EnqueuedSequenceNumber"] = new(typeof(long), m => m.SequenceNumber),
        // Filters run before the first delivery, and Azure already reports 1 there.
        ["DeliveryCount"] = new(typeof(int), m => m.DeliveryCount + 1),
        ["EnqueuedTimeUtc"] = new(typeof(DateTime), m => m.EnqueuedTimeUtc.UtcDateTime),
        ["ExpiresAtUtc"] = new(typeof(DateTime), ExpiresAt),
        ["Size"] = new(typeof(long), m => (long)m.Body.Length),
        ["LockToken"] = new(typeof(Guid), _ => Guid.Empty),
        ["LockedUntilUtc"] = new(typeof(DateTime), _ => DateTime.MinValue),
        ["DeadLetterSource"] = new(typeof(string), _ => null),
        ["State"] = new(typeof(MessageState), m => m.State),
        ["ForcePersistence"] = new(typeof(bool), _ => false),
    };

    /// <summary>
    /// When the sender already set one of these, Azure keeps the sender's value on the delivered message even
    /// after an action <c>SET</c>s it (later statements still read the new value).
    /// </summary>
    public static readonly IReadOnlySet<string> SenderPresetWins = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "To", "ReplyTo", "CorrelationId", "ReplyToSessionId", "MessageId", "SessionId"
    };

    public static bool IsReadable(string name) => Properties.ContainsKey(name);

    public static bool IsSettable(string name) => Properties.TryGetValue(name, out var p) && p.Set is not null;

    /// <summary>The CLR type an action's value is converted to before <see cref="Set"/>.</summary>
    public static Type TypeOf(string name) => Properties[name].Type;

    public static object? Get(BrokeredMessage message, string name) => Properties[name].Get(message);

    /// <summary>Sets a settable property; <paramref name="value"/> must already be of <see cref="TypeOf"/>.</summary>
    public static void Set(BrokeredMessage message, string name, object? value) => Properties[name].Set!(message, value);

    private static object ExpiresAt(BrokeredMessage m)
    {
        var enqueued = m.EnqueuedTimeUtc.UtcDateTime;
        return m.TimeToLive >= DateTime.MaxValue - enqueued ? DateTime.MaxValue : enqueued + m.TimeToLive;
    }
}
