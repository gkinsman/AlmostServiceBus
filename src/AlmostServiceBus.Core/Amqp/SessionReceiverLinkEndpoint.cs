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
                    link.Session.Connection.Closed += (_, __) => cts.Cancel();
                    _pumpTask = Task.Run(() => SessionPumpAsync(link, cts.Token));
                }
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnFlow failed for session '{SessionId}'", _session.SessionId);
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
            Log.LogWarning(ex, "SessionPumpAsync: unexpected error for session '{SessionId}'", _session.SessionId);
        }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
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
            try
            {
                dispositionContext.Link.DisposeMessage(dispositionContext.Message, new Rejected
                {
                    Error = new Error(new Symbol("com.microsoft:message-lock-lost"))
                    {
                        Description = "The lock supplied is invalid. Either the lock expired, or the message has already been removed from the queue."
                    }
                }, true);
            }
            catch (Exception ex)
            {
                Log.LogDebug(ex, "Failed to send lock-lost rejection for session '{SessionId}'", _session.SessionId);
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnDisposition failed for session '{SessionId}', lock={LockToken}", _session.SessionId, lockToken);
        }
    }

    public override void OnLinkClosed(ListenerLink link, Error error)
    {
        try
        {
            _pumpCts?.Cancel();
            _pumpCts?.Dispose();
            _pumpCts = null;

            // Release the session lock so a new receiver can pick up this session
            // immediately. Under high load (e.g. connection resets), holding the lock
            // for the full LockDuration (30s) starves new receivers and causes messages
            // to pile up. The RenewSessionLock 404 race (see below) is acceptable
            // because if the link is closing, the client is already tearing down.
            //
            // Note: in normal SDK-initiated close, the SDK releases the session lock
            // explicitly via $management before closing the link. This path handles
            // abnormal closes (connection resets, timeouts).
            try
            {
                _queue.Sessions?.ReleaseSession(_session.SessionId);
                Log.LogDebug("Released session lock for session '{SessionId}' on link close (error={Error})",
                    _session.SessionId, error?.Description);
            }
            catch (Exception ex)
            {
                Log.LogDebug(ex, "Failed to release session lock for '{SessionId}'", _session.SessionId);
            }

            // Reclaim any pending (in-flight) messages back to the session queue.
            // When the pump was running, it dequeued messages from the session and
            // tracked them in _queue._pending. With the link dead, those messages
            // would be stuck in _pending until SweepExpiredLocks picks them up (up
            // to LockDuration delay). Reclaiming them immediately allows the next
            // receiver to process them without waiting.
            try
            {
                _queue.ReclaimPendingForSession(_session.SessionId);
            }
            catch (Exception ex)
            {
                Log.LogDebug(ex, "Failed to reclaim pending messages for session '{SessionId}'", _session.SessionId);
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnLinkClosed failed for session '{SessionId}'", _session.SessionId);
        }

        base.OnLinkClosed(link, error);
    }
}
