using AlmostServiceBus.Core.Amqp;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// Expires messages whose time-to-live has passed in every queue and subscription of every
/// namespace, so they leave the active count (and reach the dead-letter queue when the entity asks
/// for that) even when nobody is receiving. Receivers expire messages they meet on their own; this
/// sweep covers the ones nobody comes for. A background <see cref="PeriodicTimer"/> drives it.
/// </summary>
public sealed class MessageExpiryProcessor : IDisposable
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<MessageExpiryProcessor>();

    private readonly NamespaceRegistry _registry;
    private readonly Lock _lifetimeLock = new();
    private CancellationTokenSource? _cts;
    private Task? _backgroundTask;

    public MessageExpiryProcessor(NamespaceRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Sweeps every entity once and returns how many messages it expired. An entity that fails is
    /// logged and skipped, so one bad entity can't stop expiry everywhere else.
    /// </summary>
    public int ProcessExpiredMessages()
    {
        var expired = 0;
        foreach (var name in _registry.ListNamespaces())
        {
            if (_registry.Get(name) is not { } ns)
                continue;

            foreach (var queue in ns.GetQueues())
                expired += Sweep(ns, queue);

            foreach (var topic in ns.GetTopics())
            foreach (var subscription in topic.GetSubscriptions())
                expired += Sweep(ns, subscription.Queue);
        }

        return expired;
    }

    private static int Sweep(NamespaceContext ns, QueueEntity queue)
    {
        try
        {
            return queue.ExpireMessages();
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "Expiring messages on '{Entity}' in namespace '{Namespace}' failed; retrying on the next tick.",
                queue.Name, ns.Name);
            return 0;
        }
    }

    /// <summary>
    /// Starts a background task that calls <see cref="ProcessExpiredMessages"/> on the given interval.
    /// </summary>
    public void StartBackground(TimeSpan interval)
    {
        lock (_lifetimeLock)
        {
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
                        try
                        {
                            ProcessExpiredMessages();
                        }
                        catch (Exception ex)
                        {
                            // An escaped exception would end this loop and silently stop all expiry.
                            Log.LogError(ex, "Message expiry failed; retrying on the next tick.");
                        }
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

            try { _backgroundTask?.Wait(TimeSpan.FromSeconds(5)); } catch { /* ignore */ }
        }
    }
}
