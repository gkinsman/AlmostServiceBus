using System.Collections.Concurrent;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// Stores scheduled messages and delivers them to their target entity when
/// their <see cref="BrokeredMessage.ScheduledEnqueueTimeUtc"/> arrives.
/// A background <see cref="PeriodicTimer"/> polls at a configurable interval.
/// </summary>
public sealed class ScheduledMessageProcessor : IDisposable
{
    private readonly record struct ScheduledKey(string NamespaceName, long SequenceNumber);
    private record ScheduledEntry(string EntityName, BrokeredMessage Message, NamespaceContext Namespace);

    private readonly NamespaceContext _defaultNamespace;
    private readonly ConcurrentDictionary<ScheduledKey, ScheduledEntry> _scheduled = new();

    private readonly Lock _lifetimeLock = new();
    private CancellationTokenSource? _cts;
    private Task? _backgroundTask;

    public ScheduledMessageProcessor(NamespaceContext namespaceContext)
    {
        _defaultNamespace = namespaceContext;
    }

    /// <summary>
    /// Assigns a sequence number, stores the message for deferred delivery, and returns the sequence number.
    /// Uses the default namespace context for entity resolution at delivery time.
    /// </summary>
    public long Schedule(string entityName, BrokeredMessage message)
    {
        return Schedule(entityName, message, _defaultNamespace);
    }

    /// <summary>
    /// Assigns a sequence number, stores the message for deferred delivery, and returns the sequence number.
    /// The supplied <paramref name="namespaceContext"/> is used to resolve the target entity at delivery time,
    /// ensuring scheduled messages are delivered to the correct namespace when namespace isolation is active.
    /// </summary>
    public long Schedule(string entityName, BrokeredMessage message, NamespaceContext namespaceContext)
    {
        var seqNo = namespaceContext.NextSequenceNumber();
        message.SequenceNumber = seqNo;
        _scheduled[new ScheduledKey(namespaceContext.Name, seqNo)] = new ScheduledEntry(entityName, message, namespaceContext);
        return seqNo;
    }

    /// <summary>
    /// Cancels a previously scheduled message. Returns <see langword="true"/> if found and removed.
    /// </summary>
    public bool CancelScheduled(long sequenceNumber) =>
        CancelScheduled(sequenceNumber, _defaultNamespace);

    public bool CancelScheduled(long sequenceNumber, NamespaceContext namespaceContext) =>
        _scheduled.TryRemove(new ScheduledKey(namespaceContext.Name, sequenceNumber), out _);

    /// <summary>
    /// Returns scheduled messages for the given entity in the given namespace, optionally
    /// filtered by minimum sequence number. Used by PeekMessage to surface scheduled
    /// messages alongside active messages.
    /// </summary>
    public IEnumerable<BrokeredMessage> GetScheduledForEntity(string namespaceName, string entityName, long fromSequenceNumber = 0)
    {
        foreach (var (key, entry) in _scheduled)
        {
            if (!string.Equals(key.NamespaceName, namespaceName, StringComparison.OrdinalIgnoreCase))
                continue;
            if (!string.Equals(entry.EntityName, entityName, StringComparison.OrdinalIgnoreCase))
                continue;
            if (entry.Message.SequenceNumber < fromSequenceNumber)
                continue;
            yield return entry.Message;
        }
    }

    /// <summary>
    /// Looks up a single scheduled message by sequence number across all namespaces/entities.
    /// </summary>
    public BrokeredMessage? GetScheduledBySequence(string namespaceName, long sequenceNumber)
    {
        if (_scheduled.TryGetValue(new ScheduledKey(namespaceName, sequenceNumber), out var entry))
            return entry.Message;
        return null;
    }

    /// <summary>
    /// Every scheduled message in the given namespace, soonest first. Used by the dashboard's
    /// admin view; <paramref name="entityName"/> narrows it to one queue or topic.
    /// </summary>
    public IReadOnlyList<ScheduledMessage> ListScheduled(string namespaceName, string? entityName = null) =>
        _scheduled
            .Where(kv => string.Equals(kv.Key.NamespaceName, namespaceName, StringComparison.OrdinalIgnoreCase))
            .Where(kv => entityName is null || string.Equals(kv.Value.EntityName, entityName, StringComparison.OrdinalIgnoreCase))
            .Select(kv => new ScheduledMessage(kv.Value.EntityName, kv.Value.Message))
            .OrderBy(m => m.Message.ScheduledEnqueueTimeUtc ?? DateTimeOffset.MinValue)
            .ThenBy(m => m.Message.SequenceNumber)
            .ToList();

    // Admin operations are namespace-wide on purpose. Anything addressing one message
    // (cancel, or cancel + reschedule) is the client's job over AMQP $management; these only
    // cover what AMQP has no operation for.

    /// <summary>
    /// Shifts every scheduled message in the namespace (optionally only those targeting
    /// <paramref name="entityName"/>) by <paramref name="offset"/>. A negative offset brings
    /// them forward; anything pushed into the past is delivered on the next poll.
    /// Returns how many messages were moved.
    /// </summary>
    public int Shift(string namespaceName, TimeSpan offset, string? entityName = null)
    {
        var now = DateTimeOffset.UtcNow;
        var moved = 0;
        foreach (var scheduled in ListScheduled(namespaceName, entityName))
        {
            var key = new ScheduledKey(namespaceName, scheduled.Message.SequenceNumber);
            if (Update(key, current => (current ?? now) + offset)) moved++;
        }
        return moved;
    }

    /// <summary>
    /// Delivers every scheduled message in the namespace (optionally only those targeting
    /// <paramref name="entityName"/>) immediately, ignoring their enqueue times.
    /// Returns how many were delivered.
    /// </summary>
    public int DeliverAllNow(string namespaceName, string? entityName = null)
    {
        var delivered = 0;
        foreach (var scheduled in ListScheduled(namespaceName, entityName))
        {
            // Skip anything the poller or a client cancel removed since the listing.
            if (TryResolveKey(namespaceName, scheduled.Message.SequenceNumber, out var key) &&
                _scheduled.TryRemove(key, out var entry))
            {
                Deliver(entry);
                delivered++;
            }
        }
        return delivered;
    }

    /// <summary>
    /// Changes a scheduled message's enqueue time without racing the delivery loop.
    /// </summary>
    /// <remarks>
    /// The entry is taken out of the store before its time is touched, so <c>TryRemove</c>
    /// arbitrates: if the poller removed it first it has been delivered and this reports
    /// <see langword="false"/>; if this removed it first the poller cannot see it while the
    /// time changes. It goes back under the same key, so the sequence number is unchanged and
    /// the client's <c>CancelScheduledMessageAsync</c> still finds it.
    /// </remarks>
    private bool Update(ScheduledKey key, Func<DateTimeOffset?, DateTimeOffset> newTime)
    {
        if (!TryResolveKey(key.NamespaceName, key.SequenceNumber, out key) || !_scheduled.TryRemove(key, out var entry))
            return false;
        entry.Message.ScheduledEnqueueTimeUtc = newTime(entry.Message.ScheduledEnqueueTimeUtc);
        _scheduled[key] = entry;
        return true;
    }

    /// <summary>
    /// Keys hold the namespace name exactly as the scheduling client sent it, while the
    /// dashboard may spell it differently; match case-insensitively like <see cref="ListScheduled"/>.
    /// </summary>
    private bool TryResolveKey(string namespaceName, long sequenceNumber, out ScheduledKey key)
    {
        key = new ScheduledKey(namespaceName, sequenceNumber);
        if (_scheduled.ContainsKey(key)) return true;
        foreach (var candidate in _scheduled.Keys)
        {
            if (candidate.SequenceNumber == sequenceNumber &&
                string.Equals(candidate.NamespaceName, namespaceName, StringComparison.OrdinalIgnoreCase))
            {
                key = candidate;
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Checks all scheduled entries and delivers any whose enqueue time has arrived.
    /// Messages with no <see cref="BrokeredMessage.ScheduledEnqueueTimeUtc"/> (or a null value)
    /// are treated as immediately due.
    /// </summary>
    public void ProcessDueMessages()
    {
        var now = DateTimeOffset.UtcNow;

        foreach (var (key, entry) in _scheduled)
        {
            var scheduledTime = entry.Message.ScheduledEnqueueTimeUtc;
            if (scheduledTime.HasValue && scheduledTime.Value > now)
                continue;

            // Remove from the scheduled store; if another thread beat us here, skip.
            // Deliver the removed entry, not the enumerated one: an admin reschedule swaps it
            // out while shifting it, and the enumerated copy may carry a time that no longer applies.
            if (!_scheduled.TryRemove(key, out var removed))
                continue;

            if (removed.Message.ScheduledEnqueueTimeUtc is { } current && current > now)
            {
                // Shifted into the future between the check above and the removal.
                _scheduled.TryAdd(key, removed);
                continue;
            }

            Deliver(removed);
        }
    }

    private static void Deliver(ScheduledEntry entry)
    {
        // Clear the scheduled time before delivery
        entry.Message.ScheduledEnqueueTimeUtc = null;

        // Deliver to the resolved target using the namespace stored at schedule time
        var (queue, topic) = entry.Namespace.ResolveSendTarget(entry.EntityName);

        if (queue is not null)
            queue.Enqueue(entry.Message);
        else if (topic is not null)
            topic.Publish(entry.Message);
    }

    /// <summary>
    /// Starts a background task that calls <see cref="ProcessDueMessages"/> on the given interval.
    /// </summary>
    public void StartBackground(TimeSpan interval)
    {
        lock (_lifetimeLock)
        {
            // Cancel any existing background task before starting a new one.
            _cts?.Cancel();
            _cts?.Dispose();

            _cts = new CancellationTokenSource();
            var token = _cts.Token;

            _backgroundTask = Task.Run(async () =>
            {
                using var timer = new PeriodicTimer(interval);

                try
                {
                    while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
                    {
                        ProcessDueMessages();
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected on disposal — exit cleanly.
                }
            }, token);
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_lifetimeLock)
        {
            _cts?.Cancel();
            _cts?.Dispose();
            _cts = null;

            // Best-effort wait so callers can rely on the background task having stopped.
            try { _backgroundTask?.Wait(TimeSpan.FromSeconds(5)); } catch { /* ignore */ }
        }
    }
}

/// <summary>A message waiting in the scheduled store, with the queue or topic it will be sent to.</summary>
public sealed record ScheduledMessage(string EntityName, BrokeredMessage Message);
