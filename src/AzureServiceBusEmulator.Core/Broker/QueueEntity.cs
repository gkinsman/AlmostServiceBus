using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace AzureServiceBusEmulator.Core.Broker;

/// <summary>
/// An in-memory queue entity backed by a <see cref="Channel{T}"/>.
/// </summary>
public sealed class QueueEntity
{
    private readonly Channel<BrokeredMessage> _channel;
    private readonly ConcurrentDictionary<string, BrokeredMessage> _pending = new();
    private readonly ConcurrentDictionary<string, BrokeredMessage> _allMessages = new();
    private readonly ConcurrentDictionary<string, DateTimeOffset> _recentMessageIds = new();
    private readonly bool _isDeadLetterQueue;
    private QueueEntity? _deadLetterQueue;
    private SessionManager? _sessionManager;
    private int _messageCount;
    private long _sequenceNumber;
    private MessageEventBus? _eventBus;
    private string? _namespaceName;
    private string? _entityName;

    public QueueEntity(string name, bool isDeadLetterQueue = false)
    {
        Name = name;
        _isDeadLetterQueue = isDeadLetterQueue;

        _channel = Channel.CreateUnbounded<BrokeredMessage>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = false
        });
    }

    // --- Configuration properties ---

    public string Name { get; }

    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxDeliveryCount { get; set; } = 10;

    public bool RequiresSession { get; set; }

    /// <summary>
    /// Session manager for session-enabled queues. Created lazily when <see cref="RequiresSession"/> is true.
    /// </summary>
    public SessionManager? Sessions => RequiresSession
        ? (_sessionManager ??= new SessionManager(LockDuration))
        : null;

    public bool DeadLetteringOnMessageExpiration { get; set; }

    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;

    public bool EnableBatchedOperations { get; set; } = true;

    public long MaxSizeInMegabytes { get; set; } = 1024L;

    public string? ForwardTo { get; set; }

    public string? ForwardDeadLetteredMessagesTo { get; set; }

    public TimeSpan? AutoDeleteOnIdle { get; set; }

    public bool RequiresDuplicateDetection { get; set; }

    public TimeSpan DuplicateDetectionHistoryTimeWindow { get; set; } = TimeSpan.FromMinutes(10);

    public string? UserMetadata { get; set; }

    /// <summary>
    /// Approximate count of messages currently in the queue.
    /// </summary>
    public int MessageCount => _messageCount;

    /// <summary>
    /// Total messages that have passed through this queue (active + consumed + dead-lettered).
    /// Used by the dashboard to show queues that have had any activity.
    /// </summary>
    public int TotalMessageCount => _allMessages.Count;

    public void SetEventBus(MessageEventBus bus, string namespaceName, string entityName)
    {
        _eventBus = bus;
        _namespaceName = namespaceName;
        _entityName = entityName;
    }

    /// <summary>
    /// The dead-letter queue for this entity. Created lazily.
    /// If this instance is already a dead-letter queue, returns itself.
    /// </summary>
    public QueueEntity DeadLetterQueue
    {
        get
        {
            if (_isDeadLetterQueue)
                return this;

            return _deadLetterQueue ??= new QueueEntity($"{Name}/$deadletterqueue", isDeadLetterQueue: true);
        }
    }

    // --- Operations ---

    /// <summary>
    /// Enqueues a message, assigning a lock token if one is not already set.
    /// When duplicate detection is enabled, silently drops messages with a
    /// MessageId that was seen within the <see cref="DuplicateDetectionHistoryTimeWindow"/>.
    /// </summary>
    public void Enqueue(BrokeredMessage message)
    {
        // Session-enabled queues route messages to the SessionManager by SessionId.
        if (RequiresSession)
        {
            if (string.IsNullOrEmpty(message.SessionId))
                return; // silently drop messages without SessionId

            Sessions!.Enqueue(message);
            // Also track in _allMessages for dashboard peek
            message.LockToken ??= Guid.NewGuid().ToString();
            if (message.SequenceNumber == 0)
                message.SequenceNumber = Interlocked.Increment(ref _sequenceNumber);
            _allMessages[message.LockToken!] = message;
            Interlocked.Increment(ref _messageCount);
            _eventBus?.Publish(new MessageEvent(
                MessageEventType.Enqueued, _namespaceName ?? "", _entityName ?? "",
                message.MessageId, message.SequenceNumber, message.ContentType,
                TruncateBody(message), ExtractScalars(message),
                DateTimeOffset.UtcNow));
            return;
        }

        // Duplicate detection: silently ignore messages with recently-seen MessageIds.
        if (RequiresDuplicateDetection && !string.IsNullOrEmpty(message.MessageId))
        {
            PurgeExpiredDuplicateEntries();

            var now = DateTimeOffset.UtcNow;
            if (!_recentMessageIds.TryAdd(message.MessageId, now))
            {
                // MessageId already seen within the window — silently drop
                return;
            }
        }

        message.LockToken ??= Guid.NewGuid().ToString();
        // Ensure every message has a unique sequence number — the Azure SDK uses this
        // as the transport message ID for deduplication. Clone() resets SequenceNumber
        // to 0, so messages arriving via topic subscription forwarding need a fresh one.
        if (message.SequenceNumber == 0)
            message.SequenceNumber = Interlocked.Increment(ref _sequenceNumber);
        _channel.Writer.TryWrite(message);
        _allMessages[message.LockToken!] = message;
        Interlocked.Increment(ref _messageCount);
        _eventBus?.Publish(new MessageEvent(
            MessageEventType.Enqueued, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            TruncateBody(message), ExtractScalars(message),
            DateTimeOffset.UtcNow));
    }

    /// <summary>
    /// Removes expired entries from the duplicate detection history.
    /// </summary>
    private void PurgeExpiredDuplicateEntries()
    {
        var cutoff = DateTimeOffset.UtcNow - DuplicateDetectionHistoryTimeWindow;
        foreach (var (messageId, timestamp) in _recentMessageIds)
        {
            if (timestamp < cutoff)
                _recentMessageIds.TryRemove(messageId, out _);
        }
    }

    /// <summary>
    /// Asynchronously dequeues the next message, incrementing its delivery count
    /// and tracking it in the pending dictionary.
    /// </summary>
    public async ValueTask<BrokeredMessage> DequeueAsync(CancellationToken cancellationToken = default)
    {
        var message = await _channel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        Interlocked.Decrement(ref _messageCount);
        message.DeliveryCount++;
        message.LockedUntil = DateTimeOffset.UtcNow.Add(LockDuration);
        TrackPending(message);
        return message;
    }

    /// <summary>
    /// Non-blocking attempt to dequeue the next message. Returns null if nothing is available.
    /// </summary>
    public BrokeredMessage? TryDequeueImmediate()
    {
        if (_channel.Reader.TryRead(out var message))
        {
            Interlocked.Decrement(ref _messageCount);
            message.DeliveryCount++;
            message.LockedUntil = DateTimeOffset.UtcNow.Add(LockDuration);
            TrackPending(message);
            return message;
        }

        return null;
    }

    /// <summary>
    /// Waits asynchronously until a message is available in the queue channel.
    /// Used by the message pump to avoid busy-polling when the queue is empty.
    /// Returns <see langword="false"/> if the channel is closed (never happens in normal operation).
    /// </summary>
    public ValueTask<bool> WaitToReadAsync(CancellationToken ct = default) =>
        _channel.Reader.WaitToReadAsync(ct);

    /// <summary>
    /// Adds a message to the pending (locked) dictionary by its lock token.
    /// </summary>
    public void TrackPending(BrokeredMessage message)
    {
        if (message.LockToken is not null)
            _pending[message.LockToken] = message;
    }

    /// <summary>
    /// Completes a message, removing it from the pending dictionary.
    /// Throws <see cref="MessageLockLostException"/> if the lock has expired.
    /// </summary>
    public void Complete(string lockToken)
    {
        if (!_pending.TryRemove(lockToken, out var message))
            return;

        // Enforce lock expiry — if the lock has expired, re-enqueue the message
        // and throw so the AMQP layer can reject the disposition.
        if (message.LockedUntil != default && DateTimeOffset.UtcNow > message.LockedUntil)
        {
            Enqueue(message);
            throw new MessageLockLostException(lockToken);
        }

        // Mark as consumed but keep in _allMessages for dashboard visibility
        if (_allMessages.TryGetValue(lockToken, out var tracked))
            tracked.State = MessageState.Consumed;

        _eventBus?.Publish(new MessageEvent(
            MessageEventType.Completed, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            null, null, DateTimeOffset.UtcNow));
    }

    /// <summary>
    /// Abandons a message. If delivery count has reached <see cref="MaxDeliveryCount"/>,
    /// the message is moved to the dead-letter queue; otherwise it is re-enqueued.
    /// </summary>
    public void Abandon(string lockToken)
    {
        if (!_pending.TryRemove(lockToken, out var message))
            return;

        _eventBus?.Publish(new MessageEvent(
            MessageEventType.Abandoned, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            null, null, DateTimeOffset.UtcNow));

        if (message.DeliveryCount >= MaxDeliveryCount)
        {
            DeadLetter(message, "MaxDeliveryCountExceeded", $"Message delivery count exceeded the maximum of {MaxDeliveryCount}.");
        }
        else
        {
            Enqueue(message);
        }
    }

    /// <summary>
    /// Moves a pending message to the dead-letter queue with the given reason and description.
    /// </summary>
    public void DeadLetter(string lockToken, string? reason, string? description)
    {
        if (!_pending.TryRemove(lockToken, out var message))
            return;

        if (_allMessages.TryGetValue(lockToken, out var tracked))
            tracked.State = MessageState.DeadLettered;
        _eventBus?.Publish(new MessageEvent(
            MessageEventType.DeadLettered, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            null, null, DateTimeOffset.UtcNow));
        DeadLetter(message, reason, description);
    }

    private void DeadLetter(BrokeredMessage message, string? reason, string? description)
    {
        message.DeadLetterReason = reason;
        message.DeadLetterErrorDescription = description;
        DeadLetterQueue.Enqueue(message);
    }

    /// <summary>
    /// Renews the lock on a pending message, extending <see cref="BrokeredMessage.LockedUntil"/>
    /// by <see cref="LockDuration"/>. Returns the new <see cref="BrokeredMessage.LockedUntil"/>
    /// time, or <see langword="null"/> if the message was not found in pending.
    /// </summary>
    public DateTimeOffset? RenewLock(string lockToken)
    {
        if (!_pending.TryGetValue(lockToken, out var message))
            return null;

        message.LockedUntil = DateTimeOffset.UtcNow.Add(LockDuration);
        return message.LockedUntil;
    }

    /// <summary>
    /// Returns a snapshot of messages in the queue without removing them.
    /// </summary>
    public IReadOnlyList<BrokeredMessage> PeekMessages(int maxCount = 50)
    {
        return _allMessages.Values
            .OrderBy(m => m.SequenceNumber)
            .Take(maxCount)
            .ToList()
            .AsReadOnly();
    }

    private static string? TruncateBody(BrokeredMessage message)
    {
        if (message.Body is null || message.Body.Length == 0) return null;
        var text = Encoding.UTF8.GetString(message.Body);
        return text.Length > 500 ? text[..500] : text;
    }

    private static Dictionary<string, object>? ExtractScalars(BrokeredMessage message)
    {
        try
        {
            if (message.Body is null || message.Body.Length == 0) return null;
            var doc = JsonDocument.Parse(message.Body);
            var root = doc.RootElement;
            if (root.TryGetProperty("message", out var inner))
                root = inner;
            var scalars = new Dictionary<string, object>();
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Value.ValueKind is JsonValueKind.String)
                    scalars[prop.Name] = prop.Value.GetString()!;
                else if (prop.Value.ValueKind is JsonValueKind.Number)
                    scalars[prop.Name] = prop.Value.GetDouble();
                else if (prop.Value.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    scalars[prop.Name] = prop.Value.GetBoolean();
                if (scalars.Count >= 5) break;
            }
            return scalars.Count > 0 ? scalars : null;
        }
        catch { return null; }
    }
}

/// <summary>
/// Thrown when a settlement operation is attempted on a message whose lock has expired.
/// </summary>
public sealed class MessageLockLostException : Exception
{
    public string LockToken { get; }

    public MessageLockLostException(string lockToken)
        : base($"The lock on message with lock token '{lockToken}' has expired.")
    {
        LockToken = lockToken;
    }
}
