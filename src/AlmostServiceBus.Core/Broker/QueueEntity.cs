using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// An in-memory queue entity backed by a <see cref="Channel{T}"/>.
/// </summary>
public sealed class QueueEntity : IDisposable
{
    private readonly Channel<BrokeredMessage> _channel;
    /// <summary>
    /// Priority channel for re-enqueued messages (abandon / lock expiry).
    /// Real ASB re-delivers abandoned messages before newly published ones;
    /// draining this channel first in <see cref="TryDequeueImmediate"/> matches that behavior.
    /// </summary>
    private readonly Channel<BrokeredMessage> _redeliveryChannel;
    private readonly ConcurrentDictionary<string, BrokeredMessage> _pending = new();
    /// <summary>
    /// Messages currently in the queue or locked by a receiver, by lock token. Settled messages
    /// leave this map (see <see cref="RecordSettled"/>) so memory is bounded by the live backlog
    /// rather than by everything the queue has ever seen.
    /// </summary>
    private readonly ConcurrentDictionary<string, BrokeredMessage> _allMessages = new();
    /// <summary>
    /// The most recently completed / dead-lettered messages, oldest first, for the dashboard's
    /// history view. Capped at <see cref="RecentSettledCapacity"/>.
    /// </summary>
    private readonly ConcurrentQueue<BrokeredMessage> _recentSettled = new();
    private const int RecentSettledCapacity = 200;
    private readonly ConcurrentDictionary<long, BrokeredMessage> _deferredBySequence = new();
    private readonly ConcurrentDictionary<string, DateTimeOffset> _recentMessageIds = new();
    /// <summary>
    /// Lock tokens removed by <see cref="SweepExpiredLocks"/>. When a consumer calls
    /// <see cref="Complete"/> after the sweep already re-enqueued the message, we need
    /// to throw <see cref="MessageLockLostException"/> instead of silently returning.
    /// </summary>
    private readonly ConcurrentDictionary<string, byte> _sweptLockTokens = new();
    private readonly bool _isDeadLetterQueue;
    private QueueEntity? _deadLetterQueue;
    private SessionManager? _sessionManager;
    private int _messageCount;
    private int _totalMessageCount;
    private int _consumedCount;
    private long _sequenceNumber;
    private MessageEventBus? _eventBus;
    private string? _namespaceName;
    private string? _entityName;
    private Timer? _lockExpiryTimer;

    public QueueEntity(string name, bool isDeadLetterQueue = false)
    {
        Name = name;
        _isDeadLetterQueue = isDeadLetterQueue;

        _channel = Channel.CreateUnbounded<BrokeredMessage>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = false
        });

        _redeliveryChannel = Channel.CreateUnbounded<BrokeredMessage>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = false
        });

        // Sweep for expired locks every 5 seconds, matching real ASB behavior
        // where messages automatically become available after lock expiry.
        _lockExpiryTimer = new Timer(_ => SweepExpiredLocks(), null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
    }

    public void Dispose()
    {
        _lockExpiryTimer?.Dispose();
        _lockExpiryTimer = null;
        _deadLetterQueue?.Dispose();
    }

    // --- Configuration properties ---

    public string Name { get; }

    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxDeliveryCount { get; set; } = 10;

    public bool RequiresSession { get; set; }

    /// <summary>
    /// Session manager for session-enabled queues. Created lazily when <see cref="RequiresSession"/> is true.
    /// Thread-safe: multiple AMQP links (receiver, management) may access this concurrently.
    /// </summary>
    public SessionManager? Sessions
    {
        get
        {
            if (!RequiresSession) return null;
            if (_sessionManager is not null) return _sessionManager;
            var sm = new SessionManager(LockDuration);
            Interlocked.CompareExchange(ref _sessionManager, sm, null);
            return _sessionManager;
        }
    }

    public bool DeadLetteringOnMessageExpiration { get; set; }

    public TimeSpan DefaultMessageTimeToLive { get; set; } = TimeSpan.MaxValue;

    public bool EnablePartitioning { get; set; } = false;

    public bool EnableExpress { get; set; } = false;

    public bool EnableBatchedOperations { get; set; } = true;

    public long MaxSizeInMegabytes { get; set; } = 1024L;

    public string? ForwardTo { get; set; }

    public string? ForwardDeadLetteredMessagesTo { get; set; }

    public TimeSpan? AutoDeleteOnIdle { get; set; }

    public bool RequiresDuplicateDetection { get; set; } = false;

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
    public int TotalMessageCount => _totalMessageCount;

    /// <summary>
    /// Number of messages completed on this queue.
    /// </summary>
    public int ConsumedCount => _consumedCount;

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

            if (_deadLetterQueue is not null)
                return _deadLetterQueue;

            var dlq = new QueueEntity($"{Name}/$deadletterqueue", isDeadLetterQueue: true);
            if (Interlocked.CompareExchange(ref _deadLetterQueue, dlq, null) != null)
                dlq.Dispose(); // lost the race — dispose the duplicate (has a Timer)
            return _deadLetterQueue;
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
            {
                // Real Service Bus rejects with InvalidOperationException when sending
                // a non-session message to a session-required entity. The SDK propagates
                // this back to the caller; previously we silently dropped, which masked
                // application bugs.
                throw new InvalidOperationException(
                    $"Cannot send a message without a SessionId to session-required queue '{Name}'.");
            }
            // Assign sequence number and lock token BEFORE enqueuing to the
            // SessionManager so the PriorityQueue can order by SequenceNumber.
            // Clone() resets SequenceNumber to 0, so messages arriving via
            // topic subscription forwarding need a fresh one here.
            message.LockToken ??= Guid.NewGuid().ToString();
            if (message.SequenceNumber == 0)
                message.SequenceNumber = Interlocked.Increment(ref _sequenceNumber);

            Sessions!.Enqueue(message);
            // Also track in _allMessages for dashboard peek
            _allMessages[message.LockToken!] = message;
            Interlocked.Increment(ref _messageCount);
            Interlocked.Increment(ref _totalMessageCount);
            PublishEnqueued(message);
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
        Interlocked.Increment(ref _totalMessageCount);
        PublishEnqueued(message);
    }

    private void PublishEnqueued(BrokeredMessage message)
    {
        if (_eventBus is null) return;
        _eventBus.Publish(new MessageEvent(
            MessageEventType.Enqueued, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            TruncateBody(message), ExtractScalars(message),
            DateTimeOffset.UtcNow,
            // Snapshot so a later properties-to-modify on abandon/defer can't mutate what the
            // dashboard already received.
            ApplicationProperties: message.ApplicationProperties.Count > 0
                ? new Dictionary<string, object>(message.ApplicationProperties)
                : null,
            Subject: message.Subject,
            CorrelationId: message.CorrelationId));
    }

    /// <summary>
    /// Re-enqueues a message that was previously dequeued (lock expiry or abandon).
    /// Unlike <see cref="Enqueue"/>, this does NOT fire an SSE event because the message
    /// is not new — it is returning to the queue. Firing Enqueued here would cause the
    /// dashboard's local SSE-driven counters to drift upward on every redelivery cycle.
    /// Writes to <see cref="_redeliveryChannel"/> so that re-enqueued messages are
    /// delivered before newly published ones, matching real ASB behavior.
    /// </summary>
    private void ReEnqueue(BrokeredMessage message)
    {
        message.LockToken ??= Guid.NewGuid().ToString();
        if (message.SequenceNumber == 0)
            message.SequenceNumber = Interlocked.Increment(ref _sequenceNumber);

        _allMessages[message.LockToken!] = message;
        Interlocked.Increment(ref _messageCount);

        // Delay redelivery by 1 second to match real Azure Service Bus behavior.
        // In real ASB, network round-trip latency naturally separates the consumer's
        // completion of the faulted dispatch from the redelivered message arriving.
        // Without this delay, the emulator's in-process redelivery races with
        // MassTransit's ConsumerAgent cleanup, causing spurious R-DUPE detection.
        _ = DelayedReEnqueue(message);
    }

    private async Task DelayedReEnqueue(BrokeredMessage message)
    {
        await Task.Delay(TimeSpan.FromSeconds(1));
        if (RequiresSession)
            Sessions!.Enqueue(message);
        else
            _redeliveryChannel.Writer.TryWrite(message);
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
    /// and tracking it in the pending dictionary. Redeliveries take priority.
    /// </summary>
    public async ValueTask<BrokeredMessage> DequeueAsync(CancellationToken cancellationToken = default)
    {
        // Check redelivery channel first (priority), then normal channel
        if (_redeliveryChannel.Reader.TryRead(out var message))
        {
            // got a redelivery synchronously
        }
        else
        {
            await WaitToReadAsync(cancellationToken).ConfigureAwait(false);
            if (!_redeliveryChannel.Reader.TryRead(out message))
                message = await _channel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        BeginDelivery(message);
        return message;
    }

    /// <summary>
    /// Non-blocking attempt to dequeue the next message of <paramref name="session"/> on this
    /// session-enabled queue. The message is locked, tracked as pending and counted exactly like
    /// a non-session dequeue. Returns <see langword="false"/> if the session has no messages.
    /// </summary>
    public bool TryDequeueFromSession(SessionState session, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out BrokeredMessage? message)
    {
        if (!session.TryDequeue(out message) || message is null)
            return false;

        BeginDelivery(message);
        return true;
    }

    /// <summary>
    /// Marks a message that has just left the queue as delivered: starts its lock, bumps its
    /// delivery count, moves it to pending and takes it out of <see cref="MessageCount"/>.
    /// </summary>
    private void BeginDelivery(BrokeredMessage message)
    {
        Interlocked.Decrement(ref _messageCount);
        message.IncrementDeliveryCount();
        message.LockedUntil = DateTimeOffset.UtcNow.Add(LockDuration);
        TrackPending(message);
    }

    /// <summary>
    /// Non-blocking attempt to dequeue the next message. Returns null if nothing is available.
    /// Re-enqueued messages (abandon / lock expiry) are delivered before newly published ones,
    /// matching real Azure Service Bus behavior.
    /// </summary>
    public BrokeredMessage? TryDequeueImmediate()
    {
        if (_redeliveryChannel.Reader.TryRead(out var message) || _channel.Reader.TryRead(out message))
        {
            BeginDelivery(message);
            return message;
        }

        return null;
    }

    /// <summary>
    /// Waits asynchronously until a message is available in either the redelivery
    /// or normal channel. Used by the message pump to avoid busy-polling.
    /// </summary>
    public async ValueTask<bool> WaitToReadAsync(CancellationToken ct = default)
    {
        var redelivery = _redeliveryChannel.Reader.WaitToReadAsync(ct).AsTask();
        var normal = _channel.Reader.WaitToReadAsync(ct).AsTask();
        await Task.WhenAny(redelivery, normal);
        return !ct.IsCancellationRequested;
    }

    /// <summary>
    /// Adds a message to the pending (locked) dictionary by its lock token.
    /// </summary>
    public void TrackPending(BrokeredMessage message)
    {
        if (message.LockToken is not null)
            _pending[message.LockToken] = message;
    }

    /// <summary>
    /// Returns whether a settlement against <paramref name="lockToken"/> would currently
    /// succeed: the message is still pending and its lock has neither expired nor been swept.
    /// Used as a transaction prepare-check so a commit doesn't silently drop a settlement
    /// whose lock was lost. For session queues the session lock governs lifetime, so an
    /// individual message lock is treated as valid while it is still pending.
    /// </summary>
    public bool IsLockValid(string lockToken)
    {
        if (_sweptLockTokens.ContainsKey(lockToken))
            return false;
        if (!_pending.TryGetValue(lockToken, out var message))
            return false;
        if (RequiresSession)
            return true;
        return message.LockedUntil == default || DateTimeOffset.UtcNow <= message.LockedUntil;
    }

    /// <summary>
    /// Completes a message, removing it from the pending dictionary.
    /// Throws <see cref="MessageLockLostException"/> if the lock has expired.
    /// </summary>
    public void Complete(string lockToken)
    {
        if (!_pending.TryRemove(lockToken, out var message))
        {
            // If the sweep already re-enqueued this message, the consumer's lock
            // has expired — signal the error so the client can handle redelivery.
            if (_sweptLockTokens.TryRemove(lockToken, out _))
                throw new MessageLockLostException(lockToken);
            return;
        }

        // Enforce lock expiry — if the lock has expired, re-enqueue the message
        // and throw so the AMQP layer can reject the disposition.
        // For session-enabled queues, the session lock governs message lifetime —
        // individual message locks don't expire independently (real ASB behavior).
        if (!RequiresSession && message.LockedUntil != default && DateTimeOffset.UtcNow > message.LockedUntil)
        {
            ReEnqueueExpired(lockToken, message);
            throw new MessageLockLostException(lockToken);
        }

        RecordSettled(lockToken, message, MessageState.Consumed);
        Interlocked.Increment(ref _consumedCount);

        _eventBus?.Publish(new MessageEvent(
            MessageEventType.Completed, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            null, null, DateTimeOffset.UtcNow));
    }

    /// <summary>
    /// Abandons a message. If delivery count has reached <see cref="MaxDeliveryCount"/>,
    /// the message is moved to the dead-letter queue; otherwise it is re-enqueued.
    /// </summary>
    public void Abandon(string lockToken) => Abandon(lockToken, null);

    /// <summary>
    /// Abandons a pending message, optionally merging the supplied properties into
    /// the message's ApplicationProperties before re-delivery. Used by the SDK's
    /// AbandonMessageAsync(message, propertiesToModify) overload.
    /// </summary>
    public void Abandon(string lockToken, IDictionary<string, object>? propertiesToModify)
    {
        if (!_pending.TryRemove(lockToken, out var message))
        {
            if (_sweptLockTokens.TryRemove(lockToken, out _))
                throw new MessageLockLostException(lockToken);
            return;
        }

        if (propertiesToModify is not null)
        {
            foreach (var kvp in propertiesToModify)
                message.ApplicationProperties[kvp.Key] = kvp.Value;
        }

        _eventBus?.Publish(new MessageEvent(
            MessageEventType.Abandoned, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            null, null, DateTimeOffset.UtcNow));

        if (message.DeliveryCount >= MaxDeliveryCount)
        {
            MoveToDeadLetterQueue(lockToken, message, "MaxDeliveryCountExceeded",
                $"Message delivery count exceeded the maximum of {MaxDeliveryCount}.");
        }
        else
        {
            // Remove old _allMessages entry — ReEnqueue will create a new one with fresh lock token.
            _allMessages.TryRemove(lockToken, out _);
            // Assign a fresh lock token so the re-delivered message gets a unique AMQP
            // delivery tag. The Azure SDK tracks pending disposition operations by lock
            // token (delivery tag); reusing the old token causes
            // "A pending operation with the same identifier already exists" when the
            // client tries to settle the re-delivered message.
            message.LockToken = null;
            ReEnqueue(message);
        }
    }

    /// <summary>
    /// Moves a pending message to the dead-letter queue with the given reason and description.
    /// </summary>
    public void DeadLetter(string lockToken, string? reason, string? description) =>
        DeadLetter(lockToken, reason, description, null);

    /// <summary>
    /// Moves a pending message to the dead-letter queue with the given reason, description,
    /// and optional properties to merge into ApplicationProperties.
    /// </summary>
    public void DeadLetter(string lockToken, string? reason, string? description, IDictionary<string, object>? propertiesToModify)
    {
        if (!_pending.TryRemove(lockToken, out var message))
        {
            if (_sweptLockTokens.TryRemove(lockToken, out _))
                throw new MessageLockLostException(lockToken);
            return;
        }

        if (propertiesToModify is not null)
        {
            foreach (var kvp in propertiesToModify)
            {
                // Standard Service Bus convention: dead-letter reason/description can be
                // supplied via either the reason/description args or as well-known property
                // keys on properties-to-modify.
                if (kvp.Key == "DeadLetterReason" && reason is null)
                    reason = kvp.Value?.ToString();
                else if (kvp.Key == "DeadLetterErrorDescription" && description is null)
                    description = kvp.Value?.ToString();
                else if (kvp.Value is not null)
                    message.ApplicationProperties[kvp.Key] = kvp.Value;
            }
        }

        MoveToDeadLetterQueue(lockToken, message, reason, description);
    }

    /// <summary>
    /// Takes a settled message out of the live map and keeps a snapshot of it in the bounded
    /// recent-history ring so the dashboard can still show it. Previously settled messages
    /// stayed in <see cref="_allMessages"/> forever, so a busy queue grew without limit and
    /// every dashboard poll walked the whole history.
    /// </summary>
    /// <remarks>
    /// The history holds a <em>copy</em>. A dead-lettered message goes on to live in the
    /// dead-letter queue, where it must be <see cref="MessageState.Active"/> to be peekable;
    /// stamping <see cref="MessageState.DeadLettered"/> on the shared object made explicitly
    /// dead-lettered messages invisible to both the dashboard's DLQ view and the SDK's
    /// dead-letter peek.
    /// </remarks>
    private void RecordSettled(string lockToken, BrokeredMessage message, MessageState state)
    {
        _allMessages.TryRemove(lockToken, out _);

        _recentSettled.Enqueue(HistorySnapshot(message, state));
        while (_recentSettled.Count > RecentSettledCapacity && _recentSettled.TryDequeue(out _))
        {
        }
    }

    private static BrokeredMessage HistorySnapshot(BrokeredMessage message, MessageState state) => new()
    {
        State = state,
        MessageId = message.MessageId,
        Body = message.Body,
        ContentType = message.ContentType,
        CorrelationId = message.CorrelationId,
        SessionId = message.SessionId,
        PartitionKey = message.PartitionKey,
        Subject = message.Subject,
        ReplyTo = message.ReplyTo,
        ReplyToSessionId = message.ReplyToSessionId,
        To = message.To,
        ScheduledEnqueueTimeUtc = message.ScheduledEnqueueTimeUtc,
        TimeToLive = message.TimeToLive,
        ApplicationProperties = new Dictionary<string, object>(message.ApplicationProperties),
        SequenceNumber = message.SequenceNumber,
        DeliveryCount = message.DeliveryCount,
        DeadLetterReason = message.DeadLetterReason,
        DeadLetterErrorDescription = message.DeadLetterErrorDescription,
        DeadLetterSource = message.DeadLetterSource,
        EnqueuedTimeUtc = message.EnqueuedTimeUtc,
        LockToken = message.LockToken,
    };

    /// <summary>
    /// Single exit for every path that dead-letters a pending message (explicit dead-letter,
    /// abandon past <see cref="MaxDeliveryCount"/>, lock-expiry sweep past the limit): stamps
    /// the reason, records the message in this queue's history, and enqueues it — still
    /// <see cref="MessageState.Active"/> — in the dead-letter queue.
    /// </summary>
    private void MoveToDeadLetterQueue(string lockToken, BrokeredMessage message, string? reason, string? description)
    {
        message.DeadLetterReason = reason;
        message.DeadLetterErrorDescription = description;
        message.DeadLetterSource = Name;

        RecordSettled(lockToken, message, MessageState.DeadLettered);

        _eventBus?.Publish(new MessageEvent(
            MessageEventType.DeadLettered, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            null, null, DateTimeOffset.UtcNow));

        // Moving a message to the DLQ creates a new delivery in a different queue.
        // Reusing the old lock token can collide with an in-flight settlement for the
        // original delivery in Azure SDK clients ("A pending operation with the same
        // identifier already exists"), so force the DLQ enqueue to assign a fresh token.
        message.LockToken = null;
        message.State = MessageState.Active;
        DeadLetterQueue.Enqueue(message);
    }

    /// <summary>
    /// Defers a pending message — it stays in the queue but is no longer visible to
    /// regular receivers. The message can only be retrieved via
    /// ReceiveDeferred(sequenceNumber).
    /// </summary>
    public void Defer(string lockToken, IDictionary<string, object>? propertiesToModify = null)
    {
        if (!_pending.TryRemove(lockToken, out var message))
        {
            if (_sweptLockTokens.TryRemove(lockToken, out _))
                throw new MessageLockLostException(lockToken);
            return;
        }

        if (propertiesToModify is not null)
        {
            foreach (var kvp in propertiesToModify)
                message.ApplicationProperties[kvp.Key] = kvp.Value;
        }

        message.State = MessageState.Deferred;
        // Stash by SequenceNumber so the SDK can retrieve via ReceiveBySequenceNumber.
        _deferredBySequence[message.SequenceNumber] = message;
        // Also remove from the active _allMessages map (it's now in the deferred map).
        _allMessages.TryRemove(lockToken, out _);

        _eventBus?.Publish(new MessageEvent(
            MessageEventType.Deferred, _namespaceName ?? "", _entityName ?? "",
            message.MessageId, message.SequenceNumber, message.ContentType,
            null, null, DateTimeOffset.UtcNow));
    }

    /// <summary>
    /// Retrieves a deferred message by its sequence number. Used by the SDK's
    /// ReceiveDeferredMessagesAsync. Returns null if no deferred message exists for
    /// the given sequence number.
    /// </summary>
    public BrokeredMessage? ReceiveDeferred(long sequenceNumber)
    {
        if (!_deferredBySequence.TryGetValue(sequenceNumber, out var msg))
            return null;

        // Stamp a fresh lock token and add to pending so the consumer can settle it.
        msg.LockToken = Guid.NewGuid().ToString();
        msg.LockedUntil = DateTimeOffset.UtcNow.Add(LockDuration);
        _pending[msg.LockToken] = msg;
        return msg;
    }

    /// <summary>
    /// Returns up to <paramref name="maxCount"/> active or deferred messages with
    /// SequenceNumber >= <paramref name="fromSequenceNumber"/>. Used by the SDK's
    /// PeekMessageAsync.
    /// </summary>
    public IEnumerable<BrokeredMessage> PeekFromSequence(long fromSequenceNumber, int maxCount, string? sessionFilter = null)
    {
        var candidates = new List<BrokeredMessage>();
        foreach (var msg in _allMessages.Values)
        {
            // Peek only returns messages that haven't been settled (Active state).
            // Consumed/DeadLettered messages are filtered out — they're either gone or
            // moved to the DLQ (which is a separate entity).
            if (msg.State != MessageState.Active) continue;
            if (msg.SequenceNumber < fromSequenceNumber) continue;
            if (sessionFilter is not null && msg.SessionId != sessionFilter) continue;
            candidates.Add(msg);
        }
        foreach (var msg in _deferredBySequence.Values)
        {
            if (msg.SequenceNumber < fromSequenceNumber) continue;
            if (sessionFilter is not null && msg.SessionId != sessionFilter) continue;
            candidates.Add(msg);
        }

        candidates.Sort((a, b) => a.SequenceNumber.CompareTo(b.SequenceNumber));
        return candidates.Take(maxCount);
    }

    /// <summary>
    /// Renews the lock on a pending message, extending <see cref="BrokeredMessage.LockedUntil"/>
    /// by <see cref="LockDuration"/>. Returns the new <see cref="BrokeredMessage.LockedUntil"/>
    /// time, or <see langword="null"/> if the message was not found in pending (it was
    /// completed, abandoned, or its lock was swept and re-enqueued under a new token).
    ///
    /// Race-safe: if <see cref="SweepExpiredLocks"/> runs concurrently and moves the
    /// message out of _pending (via ReEnqueueExpired, which sets LockToken=null before
    /// re-enqueueing), the final check catches it and returns null so the caller knows
    /// the renewal is invalid.
    /// </summary>
    public DateTimeOffset? RenewLock(string lockToken)
    {
        if (!_pending.TryGetValue(lockToken, out var message))
            return null;

        var newExpiry = DateTimeOffset.UtcNow.Add(LockDuration);
        message.LockedUntil = newExpiry;

        // Race check: if the sweep concurrently re-enqueued this message between
        // our TryGetValue and our LockedUntil write, it will have cleared LockToken
        // (see ReEnqueueExpired → ReEnqueue). If the token no longer matches, our
        // renewal landed on a message that's no longer under this lock token.
        if (!string.Equals(message.LockToken, lockToken, StringComparison.Ordinal))
            return null;

        // Belt-and-braces: also confirm the message is still in _pending. If it was
        // removed (completed/abandoned/swept) after our TryGetValue, the renewal is void.
        if (!_pending.ContainsKey(lockToken))
            return null;

        return newExpiry;
    }

    /// <summary>
    /// Re-enqueues a message whose lock has expired, cleaning up the old _allMessages
    /// entry and respecting MaxDeliveryCount.
    /// </summary>
    private void ReEnqueueExpired(string oldLockToken, BrokeredMessage message)
    {
        // Remember this lock token was swept so Complete/Abandon can throw
        // MessageLockLostException instead of silently returning.
        _sweptLockTokens[oldLockToken] = 0;

        if (message.DeliveryCount >= MaxDeliveryCount)
        {
            MoveToDeadLetterQueue(oldLockToken, message, "MaxDeliveryCountExceeded",
                $"Message delivery count exceeded the maximum of {MaxDeliveryCount}.");
        }
        else
        {
            // Remove old dashboard entry — ReEnqueue will create a new one with fresh lock token.
            _allMessages.TryRemove(oldLockToken, out _);
            message.LockToken = null;
            ReEnqueue(message);
        }
    }

    /// <summary>
    /// Reclaims pending (in-flight) messages for a specific session, re-enqueuing
    /// them to the session queue so the next receiver can process them. Called when
    /// a session receiver link closes (connection reset, timeout) to avoid messages
    /// being stuck in _pending until lock expiry.
    /// </summary>
    public void ReclaimPendingForSession(string sessionId)
    {
        var reclaimed = 0;
        foreach (var (lockToken, message) in _pending)
        {
            if (string.Equals(message.SessionId, sessionId, StringComparison.OrdinalIgnoreCase)
                && _pending.TryRemove(lockToken, out var removed))
            {
                _allMessages.TryRemove(lockToken, out _);
                removed.LockToken = null;
                ReEnqueue(removed);
                reclaimed++;
            }
        }

        if (reclaimed > 0)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[QUEUE] Reclaimed {reclaimed} pending messages for session '{sessionId}' on queue '{Name}'");
        }
    }

    /// <summary>
    /// Background sweep that returns expired-lock messages to the queue,
    /// matching real Azure Service Bus behavior where messages automatically
    /// become available after lock expiry even if the consumer never settles.
    /// For session queues, this catches orphaned messages whose session lock
    /// was released but whose individual message locks haven't been settled.
    /// </summary>
    private void SweepExpiredLocks()
    {
        var now = DateTimeOffset.UtcNow;
        // Grace period: don't re-enqueue immediately when a lock expires. Auto-renewal
        // requests from the SDK may be in flight, and under high CPU load the management
        // request processing can lag by a few seconds. Without a grace period, the sweep
        // races with auto-renewal: it re-enqueues a message whose consumer is still alive
        // and about to renew, causing R-DUPE cascades.
        //
        // The grace is 10% of LockDuration (e.g. 6s for 60s locks, 3s for 30s locks,
        // proportional for shorter test locks).
        var graceTicks = Math.Max(LockDuration.Ticks / 10, TimeSpan.FromMilliseconds(1).Ticks);
        var sweepThreshold = now - new TimeSpan(graceTicks);
        foreach (var (lockToken, message) in _pending)
        {
            if (message.LockedUntil != default && sweepThreshold > message.LockedUntil)
            {
                // For session-enabled queues, the session lock governs message lifetime.
                // Only sweep messages whose session lock has ALSO expired or been released.
                // This matches real Azure Service Bus behavior where individual message
                // locks don't expire independently while the session is locked.
                if (RequiresSession && message.SessionId is not null
                    && Sessions?.IsSessionLocked(message.SessionId) == true)
                {
                    continue;
                }
                // Mark the lock token as swept BEFORE removing from _pending.
                // This closes a TOCTOU window: without this, a concurrent Complete()
                // could see the message missing from _pending (we already TryRemove'd it)
                // AND missing from _sweptLockTokens (we haven't added it yet), causing
                // Complete() to silently return while we re-enqueue — duplicate delivery.
                _sweptLockTokens[lockToken] = 0;

                // Atomically remove from pending — if another thread already removed it
                // (e.g. Complete/Abandon), TryRemove returns false and we skip.
                if (_pending.TryRemove(lockToken, out var expired))
                {
                    // Re-check: RenewLock may have extended LockedUntil between
                    // the expiry check above and the TryRemove. If the lock is now
                    // valid, put the message back — re-enqueuing a renewed message
                    // causes duplicate delivery and permanent R-DUPE cycles in
                    // MassTransit's ConsumerAgent tracking.
                    if (expired.LockedUntil > DateTimeOffset.UtcNow)
                    {
                        _sweptLockTokens.TryRemove(lockToken, out _);
                        _pending[lockToken] = expired;
                        continue;
                    }

                    ReEnqueueExpired(lockToken, expired);
                }
                else
                {
                    // Another thread already removed it (Complete/Abandon succeeded).
                    // Clean up the swept marker.
                    _sweptLockTokens.TryRemove(lockToken, out _);
                }
            }
        }
    }

    /// <summary>
    /// Returns a snapshot of messages in the queue without removing them.
    /// Active messages are shown first, then most recent consumed/dead-lettered.
    /// </summary>
    public IReadOnlyList<BrokeredMessage> PeekMessages(int maxCount = 50)
    {
        // Show active messages first (by sequence number), then recent non-active ones.
        var active = new List<BrokeredMessage>();
        foreach (var m in _allMessages.Values)
        {
            if (m.State == MessageState.Active)
                active.Add(m);
        }

        var settled = _recentSettled.ToList();

        active.Sort((a, b) => a.SequenceNumber.CompareTo(b.SequenceNumber));
        settled.Reverse(); // newest first

        var result = new List<BrokeredMessage>(Math.Min(maxCount, active.Count + settled.Count));
        result.AddRange(active.Take(maxCount));
        if (result.Count < maxCount)
            result.AddRange(settled.Take(maxCount - result.Count));

        return result.AsReadOnly();
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
