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
    private readonly bool _preSettled;
    private readonly Broker.Transactions.TransactionManager? _transactions;
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;

    public SessionReceiverLinkEndpoint(QueueEntity queue, BrokerSessionState session, bool preSettled = false, Broker.Transactions.TransactionManager? transactions = null)
    {
        _queue = queue;
        _session = session;
        _preSettled = preSettled;
        _transactions = transactions;
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

                    // ReceiveAndDelete (pre-settled) mode: auto-complete since the client
                    // never sends a disposition.
                    if (_preSettled && brokered.LockToken is not null)
                        _queue.Complete(brokered.LockToken);

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

        // Transactional settlement: buffer the real outcome under the transaction and echo a
        // transactional disposition. The message stays locked until the client commits; on
        // rollback nothing is applied and the session lock governs redelivery.
        if (dispositionContext.DeliveryState is global::Amqp.Transactions.TransactionalState txnState)
        {
            EnlistTransactionalSettlement(dispositionContext, txnState, lockToken);
            return;
        }

        try
        {
            if (lockToken is not null && dispositionContext.DeliveryState is not null)
                ApplySettlement(lockToken, dispositionContext.DeliveryState);

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

    /// <summary>
    /// Applies a settlement outcome to a pending session message. Shared by the normal and
    /// transactional disposition paths.
    /// </summary>
    private void ApplySettlement(string lockToken, DeliveryState state)
    {
        switch (state)
        {
            case Accepted:
                _queue.Complete(lockToken);
                break;
            case Released:
                _queue.Abandon(lockToken);
                break;
            case Rejected rejected:
                // Use the shared helper so the Azure SDK's DeadLetterReason /
                // DeadLetterErrorDescription (sent in Error.Info map) are
                // extracted consistently on session and non-session queues.
                var (dlReason, dlDescription) =
                    ReceiverLinkEndpoint.ExtractDeadLetterInfoStatic(rejected);
                _queue.DeadLetter(lockToken, dlReason, dlDescription,
                    ReceiverLinkEndpoint.ExtractRejectedProperties(rejected));
                break;
            case Modified modified:
                // Azure Service Bus semantics:
                //   Modified.UndeliverableHere=true  → Defer
                //   Modified.UndeliverableHere=false → Abandon with property modifications
                var modProps = ReceiverLinkEndpoint.ExtractMessageAnnotationProperties(modified);
                if (modified.UndeliverableHere == true)
                    _queue.Defer(lockToken, modProps);
                else
                    _queue.Abandon(lockToken, modProps);
                break;
            default:
                _queue.Complete(lockToken);
                break;
        }
    }

    /// <summary>
    /// Buffers a transactional settlement so it is applied only on commit, then echoes a
    /// transactional disposition.
    /// </summary>
    private void EnlistTransactionalSettlement(
        DispositionContext dispositionContext,
        global::Amqp.Transactions.TransactionalState txnState,
        string? lockToken)
    {
        if (_transactions is null || lockToken is null)
        {
            dispositionContext.Complete();
            return;
        }

        var outcome = txnState.Outcome ?? new Accepted();
        var token = lockToken;

        try
        {
            _transactions.Enlist(txnState.TxnId, commit: () =>
            {
                try
                {
                    ApplySettlement(token, outcome);
                }
                catch (MessageLockLostException)
                {
                    Log.LogWarning("Txn commit: lock {LockToken} lost before settlement on session '{SessionId}'", token, _session.SessionId);
                }
            });
        }
        catch (Broker.Transactions.TransactionNotFoundException)
        {
            dispositionContext.Link.DisposeMessage(dispositionContext.Message, new Rejected
            {
                Error = new Error(new Symbol("amqp:transaction-unknown-id"))
                {
                    Description = "Unknown or already-discharged transaction id."
                }
            }, true);
            return;
        }

        dispositionContext.Link.DisposeMessage(dispositionContext.Message, new global::Amqp.Transactions.TransactionalState
        {
            TxnId = txnState.TxnId,
            Outcome = outcome
        }, true);
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
            // to pile up.
            //
            // Note: we do NOT reclaim pending messages here. If the client's settlement
            // for a message was in-flight when the link closed, reclaiming immediately
            // would race: the settlement might arrive for the old receiver, but the
            // re-enqueued message is already being processed by a new receiver, causing
            // R-DUPE cascades via published events.
            //
            // Instead, ReclaimPendingForSession is called when a NEW receiver accepts
            // this session (see ServiceBusLinkProcessor). At that point, any pending
            // messages for this session must be from a previous (dead) receiver, so
            // reclaiming is safe.
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
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnLinkClosed failed for session '{SessionId}'", _session.SessionId);
        }

        base.OnLinkClosed(link, error);
    }
}
