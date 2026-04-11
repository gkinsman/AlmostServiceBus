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
            if (!_scheduled.TryRemove(key, out _))
                continue;

            // Clear the scheduled time before delivery
            entry.Message.ScheduledEnqueueTimeUtc = null;

            // Deliver to the resolved target using the namespace stored at schedule time
            var (queue, topic) = entry.Namespace.ResolveSendTarget(entry.EntityName);

            if (queue is not null)
                queue.Enqueue(entry.Message);
            else if (topic is not null)
                topic.Publish(entry.Message);
        }
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
