using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Logging;
using BrokerSessionState = AlmostServiceBus.Core.Broker.SessionState;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Session-aware receiver link endpoint. Locks a session on creation and
/// delivers messages only from that session's channel in FIFO order.
/// </summary>
public class SessionReceiverLinkEndpoint : LinkEndpoint
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<SessionReceiverLinkEndpoint>();
    private readonly QueueEntity _queue;
    private readonly BrokerSessionState _session;
    private readonly Lock _pumpLock = new();
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;

    public SessionReceiverLinkEndpoint(QueueEntity queue, BrokerSessionState session)
    {
        _queue = queue;
        _session = session;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // All AMQPNetLite callbacks MUST be wrapped in try-catch. An unhandled exception
        // here propagates into AMQPNetLite's frame processing and destroys the AMQP session
        // (transport channel), killing ALL links on that session. Under Black Friday load
        // this cascading failure kills the logistics-dispatch pump permanently.
        try
        {
            if (flowContext.Link.IsDraining)
            {
                flowContext.Link.CompleteDrain();
                _pumpCts?.Cancel();
                return;
            }

            lock (_pumpLock)
            {
                if (_pumpTask is null || _pumpTask.IsCompleted)
                {
                    var cts = new CancellationTokenSource();
                    _pumpCts = cts;
                    var link = flowContext.Link;
                    link.Closed += (_, __) => cts.Cancel();
                    try { link.Session.Connection.Closed += (_, __) => cts.Cancel(); }
                    catch { /* Session/Connection may be closing — ignore */ }
                    _pumpTask = Task.Run(() => SessionPumpAsync(link, cts.Token));
                }
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnFlow failed for session '{SessionId}' on queue — suppressed to protect AMQP session", _session.SessionId);
        }
    }

    private async Task SessionPumpAsync(ListenerLink link, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (link.IsDraining)
                    break;

                // Check AMQPNetLite's internal credit before dequeuing
                if (ReceiverLinkEndpoint.GetLinkCreditStatic(link) <= 0)
                {
                    await Task.Delay(1, ct);
                    continue;
                }

                if (!_session.TryDequeue(out var brokered))
                {
                    // Block until a session message is available rather than busy-polling.
                    await _session.WaitToReadAsync(ct);
                    continue;
                }

                brokered!.IncrementDeliveryCount();
                brokered.LockedUntil = DateTimeOffset.UtcNow.Add(_queue.LockDuration);
                _queue.TrackPending(brokered);

                var amqpMessage = ReceiverLinkEndpoint.ConvertToAmqpMessage(brokered);

                // Add session ID to message annotations (SDK reads this)
                amqpMessage.MessageAnnotations[new Symbol("x-opt-session-id")] = _session.SessionId;

                try
                {
                    link.SendMessage(amqpMessage);
                    await Task.Yield();
                }
                catch (Exception ex)
                {
                    // Send failed — link is closing/draining.
                    // Abandon the message so it re-enters the queue for the next consumer.
                    Log.LogDebug(ex, "Session PUMP SendMessage failed for session '{SessionId}', abandoning message {MessageId}",
                        _session.SessionId, brokered.MessageId);
                    if (brokered.LockToken is not null)
                        _queue.Abandon(brokered.LockToken);
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "Session pump died unexpectedly for session '{SessionId}' on queue '{Queue}'", _session.SessionId, _queue.Name);
        }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        // Wrapped in try-catch to prevent exceptions from killing the AMQP session.
        try
        {
            var lockToken = ReceiverLinkEndpoint.GetLockTokenStatic(dispositionContext.Message);

            try
            {
                if (lockToken is not null && dispositionContext.DeliveryState is not null)
                {
                    switch (dispositionContext.DeliveryState)
                    {
                        case Accepted:
                            _queue.Complete(lockToken);
                            break;
                        case Released:
                            _queue.Abandon(lockToken);
                            break;
                        case Rejected rejected:
                            _queue.DeadLetter(lockToken,
                                rejected.Error?.Condition?.ToString(),
                                rejected.Error?.Description);
                            break;
                        case Modified modified:
                            if (modified.UndeliverableHere == true)
                                _queue.DeadLetter(lockToken, "Undeliverable", "Message marked as undeliverable.");
                            else
                                _queue.Abandon(lockToken);
                            break;
                        default:
                            _queue.Complete(lockToken);
                            break;
                    }
                }

                dispositionContext.Complete();
            }
            catch (MessageLockLostException)
            {
                Log.LogDebug("DISP lock={LockToken} LOCK EXPIRED (re-enqueued) queue='{Queue}'", lockToken, _queue.Name);
                dispositionContext.Link.DisposeMessage(dispositionContext.Message, new Rejected
                {
                    Error = new Error(new Symbol("com.microsoft:message-lock-lost"))
                    {
                        Description = "The lock supplied is invalid. Either the lock expired, or the message has already been removed from the queue."
                    }
                }, true);
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnDisposition failed for session '{SessionId}' — suppressed to protect AMQP session", _session.SessionId);
        }
    }

    public override void OnLinkClosed(ListenerLink link, Error error)
    {
        try
        {
            _pumpCts?.Cancel();
            _pumpCts?.Dispose();
            _pumpCts = null;

            // Release the session lock so another consumer can pick it up immediately.
            // Without this, the lock persists for the full LockDuration (e.g. 30s) after
            // the link closes, which under sustained Black Friday load causes cascading
            // session unavailability — all sessions become locked, new receivers can't
            // accept any session, and the queue stalls completely.
            //
            // The Azure SDK's auto-renewal timer may fire briefly after the link closes,
            // but the management endpoint handles renew-on-unlocked-session gracefully
            // (returns the current time rather than erroring), so this is safe.
            _queue.Sessions?.ReleaseSession(_session.SessionId);

            // Reclaim any unsettled messages for this session. These are messages that
            // were sent to the consumer but not yet completed/abandoned when the link
            // died (e.g. AMQP connection reset under load). Without this, they stay
            // stuck in _pending forever because SweepExpiredLocks skips session queues.
            _queue.ReclaimPendingForSession(_session.SessionId);

            base.OnLinkClosed(link, error);
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnLinkClosed failed for session '{SessionId}' — suppressed to protect AMQP session", _session.SessionId);
        }
    }
}
