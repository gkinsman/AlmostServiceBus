using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Server-side endpoint for sending messages to clients.
/// When a client has a receiver link, the server has a sender endpoint.
///
/// The Azure SDK grants credit upfront and expects messages to be pushed
/// as they arrive. We start a background pump that continuously dequeues
/// from the queue and sends to the client while credit is available.
/// </summary>
public class ReceiverLinkEndpoint : LinkEndpoint
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<ReceiverLinkEndpoint>();
    private readonly QueueEntity _queue;
    private readonly Lock _pumpLock = new();
    private readonly bool _preSettled;
    private readonly Broker.Transactions.TransactionManager? _transactions;
    private readonly CreditShadow _credit = new();
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;

    public ReceiverLinkEndpoint(QueueEntity queue, bool preSettled = false, Broker.Transactions.TransactionManager? transactions = null)
    {
        _queue = queue;
        _preSettled = preSettled;
        _transactions = transactions;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        try
        {
            Log.LogDebug("FLOW queue='{Queue}' credit={Credit} drain={Drain}", _queue.Name, flowContext.Messages, flowContext.Link.IsDraining);

            // When the client sends drain=true, it wants to stop receiving.
            // Cancel the pump first, then complete the drain so the response
            // Flow frame (credit=0) is sent after the pump has stopped sending.
            if (flowContext.Link.IsDraining)
            {
                // Complete the drain IMMEDIATELY — send Flow(credit=0) back to the
                // consumer before doing anything else. If we delay (e.g. waiting for
                // the pump to stop), the link may start detaching and CompleteDrain's
                // internal SendFlow becomes a no-op (it checks !IsDetaching).
                flowContext.Link.CompleteDrain();
                _credit.Reset();
                _pumpCts?.Cancel();
                return;
            }

            _credit.OnFlow(flowContext);

            lock (_pumpLock)
            {
                if (_pumpTask is null || _pumpTask.IsCompleted)
                {
                    var cts = new CancellationTokenSource();
                    _pumpCts = cts;
                    var link = flowContext.Link;

                    // Capture cts local — not the _pumpCts field — so that if a new
                    // pump starts later (overwriting _pumpCts), the old link's Closed
                    // handler cancels THIS pump's CTS, not the new one.
                    link.Closed += (_, __) => cts.Cancel();
                    link.Session.Connection.Closed += (_, __) => cts.Cancel();

                    _pumpTask = Task.Run(() => MessagePumpAsync(link, cts.Token));
                }
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnFlow failed for queue '{Queue}'", _queue.Name);
        }
    }

    private async Task MessagePumpAsync(ListenerLink link, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Stop if the link is draining (client wants to close).
                if (link.IsDraining)
                    break;

                // Check AMQPNetLite's internal credit before dequeuing.
                // The credit field is updated by AMQPNetLite when the client
                // sends Flow frames (including after completing messages).
                if (GetLinkCredit(link) <= 0)
                {
                    await Task.Delay(1, ct);
                    continue;
                }

                var brokered = _queue.TryDequeueImmediate();
                if (brokered is null)
                {
                    // Block until a message is enqueued rather than busy-polling.
                    // WaitToReadAsync wakes up immediately when Enqueue() or Abandon()
                    // writes to the channel, so re-delivery after an abandon is instant.
                    await _queue.WaitToReadAsync(ct);
                    continue;
                }

                try
                {
                    Log.LogDebug("PUMP {MessageId} → '{Queue}'", brokered.MessageId, _queue.Name);
                    var amqpMessage = ConvertToAmqpMessage(brokered);
                    link.SendMessage(amqpMessage);
                    _credit.OnSent();

                    // ReceiveAndDelete (pre-settled) mode: auto-complete the message on
                    // the broker side since the client never sends a disposition. Without
                    // this, the message stays in _pending forever and PeekMessage still
                    // returns it.
                    if (_preSettled && brokered.LockToken is not null)
                        _queue.Complete(brokered.LockToken);

                    // Yield after each send to let the AMQP stack process the transfer
                    // frame and update link credit. Without this, the pump loop can blast
                    // messages faster than the credit is decremented, causing the consumer's
                    // ServiceBusProcessor to release messages it can't process yet.
                    await Task.Yield();
                }
                catch (Exception ex)
                {
                    // Send failed — link is closing/draining.
                    // Abandon the message so it re-enters the queue for the next consumer.
                    // (Using Abandon removes it from _pending AND re-enqueues, avoiding duplicates.)
                    Log.LogDebug(ex, "PUMP SendMessage failed for '{Queue}', abandoning message {MessageId}", _queue.Name, brokered.MessageId);
                    if (brokered.LockToken is not null)
                        _queue.Abandon(brokered.LockToken);
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "MessagePumpAsync: unexpected error for queue '{Queue}'", _queue.Name);
        }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        var lockToken = GetLockTokenStatic(dispositionContext.Message);
        var stateInfo = dispositionContext.DeliveryState switch
        {
            Rejected r => $"Rejected: {r.Error?.Condition} {r.Error?.Description}",
            Modified m => $"Modified: undeliverable={m.UndeliverableHere} failed={m.DeliveryFailed}",
            _ => dispositionContext.DeliveryState?.GetType().Name ?? "null"
        };
        Log.LogDebug("DISP lock={LockToken} state={State} queue='{Queue}'", lockToken, stateInfo, _queue.Name);

        if (IsTeardownOutcome(dispositionContext))
        {
            Log.LogDebug("DISP lock={LockToken} ignored: link closed, outcome synthesised by teardown queue='{Queue}'", lockToken, _queue.Name);
            return;
        }

        // Transactional settlement: the disposition's delivery-state carries a txn-id and an
        // inner outcome. Buffer the real settlement under that transaction and echo a
        // transactional disposition. The message stays locked until the client commits; on
        // rollback nothing is applied and the lock simply expires (redelivery bumps DeliveryCount).
        if (dispositionContext.DeliveryState is global::Amqp.Transactions.TransactionalState txnState)
        {
            EnlistTransactionalSettlement(dispositionContext, txnState, lockToken);
            return;
        }

        try
        {
            if (lockToken is not null && dispositionContext.DeliveryState is not null)
                SettleMessage(lockToken, dispositionContext.DeliveryState);

            SettleWithClientOutcome(dispositionContext);
        }
        catch (MessageLockLostException)
        {
            // The message lock has expired and the message has been re-enqueued.
            // Send a Rejected disposition with com.microsoft:message-lock-lost so
            // the Azure SDK raises ServiceBusException(Reason=MessageLockLost).
            // We use Link.DisposeMessage instead of dispositionContext.Complete(Error)
            // because the latter detaches the entire link.
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
                Log.LogDebug(ex, "Failed to send lock-lost rejection for queue '{Queue}'", _queue.Name);
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "OnDisposition failed for queue '{Queue}', lock={LockToken}", _queue.Name, lockToken);
        }
    }

    /// <summary>
    /// Buffers a transactional settlement (complete/abandon/dead-letter/defer) so it is
    /// applied only when the client commits the transaction, then echoes a transactional
    /// disposition. Mirrors <see cref="SenderLinkEndpoint"/>'s transactional-send handling.
    /// </summary>
    private void EnlistTransactionalSettlement(
        DispositionContext dispositionContext,
        global::Amqp.Transactions.TransactionalState txnState,
        string? lockToken)
    {
        if (_transactions is null || lockToken is null)
        {
            // No manager (shouldn't happen — coordinator links are rejected then) or no lock
            // token to act on: fall back to settling normally so the link doesn't stall.
            dispositionContext.Complete();
            return;
        }

        // A transactional disposition always names the real intent in its inner outcome;
        // default to Accepted (complete) if it is somehow absent.
        var outcome = txnState.Outcome ?? new Accepted();
        var token = lockToken;

        try
        {
            // prepare: validated before any operation in the transaction is applied, so a
            // settlement whose lock was lost rolls the whole commit back instead of being
            // silently dropped while the discharge still reports success.
            _transactions.Enlist(txnState.TxnId,
                commit: () => SettleMessage(token, outcome),
                rollback: null,
                prepare: () => _queue.IsLockValid(token));
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

    /// <summary>
    /// Settles the client's disposition with the outcome the real service would reply with.
    /// </summary>
    /// <remarks>
    /// <c>DispositionContext.Complete()</c> always settles with <see cref="Accepted"/>. The Java
    /// SDK checks that the broker's reply has the same outcome type as its request and fails an
    /// abandon, defer or dead-letter (<see cref="Modified"/>/<see cref="Released"/>/
    /// <see cref="Rejected"/>) that comes back as <c>Accepted{}</c>; the .NET SDK happens not to
    /// check the type. The dead-letter reply must be a <em>bare</em> Rejected: the .NET SDK throws
    /// when the broker's Rejected carries an <c>Error</c> (that is how the service reports a
    /// refused settlement), so the client's error map (dead-letter reason/description) must not
    /// be echoed back.
    /// </remarks>
    internal static void SettleWithClientOutcome(DispositionContext dispositionContext)
    {
        Outcome reply = dispositionContext.DeliveryState switch
        {
            Modified m => new Modified { DeliveryFailed = m.DeliveryFailed, UndeliverableHere = m.UndeliverableHere },
            Released r => r,
            Rejected => new Rejected(),
            _ => new Accepted(),
        };
        dispositionContext.Link.DisposeMessage(dispositionContext.Message, reply, true);
    }

    /// <summary>
    /// Shadow of AMQPNetLite's link credit, used to undo a quirk in its flow handling.
    /// </summary>
    /// <remarks>
    /// <c>ListenerLink.OnFlow</c> computes <c>delta = peerLimit - ourLimit</c> and treats
    /// <c>delta &lt;= 0</c> as "peer reduced credit", setting credit to zero. A client that
    /// re-sends an identical Flow (same delivery-count and link-credit) produces <c>delta == 0</c>
    /// and loses all its credit. The Python SDK (pyamqp) does exactly that on every
    /// <c>receive_messages</c> call, so a receiver that had one credit outstanding never got its
    /// message. We track the credit we believe the peer granted and, on a zero-delta flow, put
    /// it back.
    /// </remarks>
    internal sealed class CreditShadow
    {
        private long _credit;

        /// <summary>Call from <see cref="LinkEndpoint.OnFlow"/> for non-drain flows.</summary>
        public void OnFlow(FlowContext flowContext)
        {
            var delta = flowContext.Messages;
            if (delta > 0)
            {
                _credit += delta;
            }
            else if (delta < 0)
            {
                _credit = 0;
            }
            else if (_credit > 0 && GetLinkCredit(flowContext.Link) == 0)
            {
                // Same limit restated by the peer: AMQPNetLite zeroed the credit, we did not lose any.
                Log.LogDebug("FLOW restated identical limit; restoring credit={Credit}", _credit);
                SetLinkCredit(flowContext.Link, (uint)_credit);
            }
        }

        /// <summary>Call after each successful <see cref="ListenerLink.SendMessage(Message)"/>.</summary>
        public void OnSent()
        {
            if (_credit > 0) _credit--;
        }

        public void Reset() => _credit = 0;
    }

    private static void SetLinkCredit(ListenerLink link, uint credit)
    {
        try { CreditField?.SetValue(link, credit); }
        catch (Exception ex) { Log.LogDebug(ex, "Failed to restore link credit via reflection"); }
    }

    /// <summary>
    /// Whether a disposition was manufactured by AMQPNetLite tearing the link down rather than
    /// sent by the client.
    /// </summary>
    /// <remarks>
    /// When a link is aborted — the client detached it, ended the AMQP session, or the connection
    /// dropped — AMQPNetLite's <c>Session.AbortLinks</c> first aborts the link and then calls
    /// <c>Delivery.ReleaseAll</c> on every unsettled outgoing delivery, which reaches
    /// <see cref="OnDisposition"/> as <see cref="Released"/> (or <see cref="Rejected"/> when the
    /// close carried an error). Treating those as client settlements abandoned — or dead-lettered —
    /// every message the consumer had in flight or prefetched, re-enqueued them a second later,
    /// and the reconnected consumer received them again while its handlers for the first delivery
    /// were still running. Real Service Bus does no such thing: a lost link leaves the messages
    /// locked until the lock expires. Because the link is aborted before the deliveries are
    /// released, <see cref="global::Amqp.AmqpObject.IsClosed"/> identifies the synthetic outcomes.
    /// </remarks>
    internal static bool IsTeardownOutcome(DispositionContext dispositionContext) =>
        dispositionContext.Link.IsClosed;

    public override void OnLinkClosed(ListenerLink link, Error error)
    {
        try
        {
            _pumpCts?.Cancel();
            _pumpCts?.Dispose();
            _pumpCts = null;
        }
        catch (Exception ex)
        {
            Log.LogDebug(ex, "OnLinkClosed cleanup failed for queue '{Queue}'", _queue.Name);
        }
        base.OnLinkClosed(link, error);
    }

    public void SettleMessage(string lockToken, DeliveryState deliveryState)
    {
        switch (deliveryState)
        {
            case Accepted:
                _queue.Complete(lockToken);
                break;
            case Released:
                _queue.Abandon(lockToken);
                break;
            case Rejected rejected:
                var (dlReason, dlDescription) = ExtractDeadLetterInfoStatic(rejected);
                _queue.DeadLetter(lockToken, dlReason, dlDescription, ExtractRejectedProperties(rejected));
                break;
            case Modified modified:
                // Azure Service Bus semantics:
                //   Modified.UndeliverableHere=true  → Defer (NOT DeadLetter)
                //   Modified.UndeliverableHere=false → Abandon with property modifications
                var modProps = ExtractMessageAnnotationProperties(modified);
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
    /// Extracts the dead-letter reason and description from a Rejected delivery state.
    /// The Azure SDK sends dead-letter reason/description in the Error.Info map
    /// (Condition is "com.microsoft:dead-letter", and Info contains the user-specified
    /// "DeadLetterReason" and "DeadLetterErrorDescription"). AMQPNetLite deserializes
    /// Info map keys as Symbol, so we iterate and compare via ToString().
    /// </summary>
    internal static (string? Reason, string? Description) ExtractDeadLetterInfoStatic(Rejected rejected)
    {
        string? dlReason = rejected.Error?.Condition?.ToString();
        string? dlDescription = rejected.Error?.Description;
        if (rejected.Error?.Info is { } info)
        {
            foreach (var (keyStr, value) in Entries(info))
            {
                if (keyStr == "DeadLetterReason" && value is string reason)
                    dlReason = reason;
                if (keyStr == "DeadLetterErrorDescription" && value is string desc)
                    dlDescription = desc;
            }
        }
        return (dlReason, dlDescription);
    }

    /// <summary>
    /// Enumerates an AMQP map's entries without going through its indexer.
    /// </summary>
    /// <remarks>
    /// <see cref="Fields"/> (used for <c>Error.Info</c> and message annotations) enforces
    /// <see cref="Symbol"/> keys in its indexer, but peers are free to send string keys — the
    /// Node.js SDK (rhea) does for dead-letter reason/description. Indexing then throws inside
    /// <see cref="OnDisposition"/>, the settlement is never sent, and the client's dead-letter
    /// call times out. Enumerating the pairs sidesteps the key-type check.
    /// </remarks>
    private static IEnumerable<(string Key, object? Value)> Entries(Map map)
    {
        foreach (var kvp in (IEnumerable<KeyValuePair<object, object>>)map)
        {
            var key = kvp.Key?.ToString();
            if (!string.IsNullOrEmpty(key))
                yield return (key, kvp.Value);
        }
    }

    /// <summary>
    /// Extracts the property modifications carried in a Modified disposition's MessageAnnotations.
    /// The Azure SDK puts properties-to-modify here for both Defer and Abandon (with mods).
    /// Returns null if there are no annotations.
    /// </summary>
    internal static IDictionary<string, object>? ExtractMessageAnnotationProperties(Modified modified)
    {
        var fields = modified.MessageAnnotations;
        if (fields is null) return null;
        var dict = new Dictionary<string, object>();
        foreach (var (key, value) in Entries(fields))
        {
            if (value is not null)
                dict[key] = value;
        }
        return dict.Count > 0 ? dict : null;
    }

    /// <summary>
    /// Extracts user-supplied properties-to-modify from a Rejected outcome's Error.Info,
    /// excluding the well-known DeadLetter* keys (which become reason/description).
    /// </summary>
    internal static IDictionary<string, object>? ExtractRejectedProperties(Rejected rejected)
    {
        if (rejected.Error?.Info is not { } info) return null;
        var dict = new Dictionary<string, object>();
        foreach (var (key, value) in Entries(info))
        {
            if (key == "DeadLetterReason" || key == "DeadLetterErrorDescription") continue;
            if (value is not null)
                dict[key] = value;
        }
        return dict.Count > 0 ? dict : null;
    }

    public async Task<BrokeredMessage> DequeueAsync(CancellationToken cancellationToken = default)
    {
        return await _queue.DequeueAsync(cancellationToken);
    }

    public static Message ConvertToAmqpMessage(BrokeredMessage brokered)
    {
        var lockGuid = Guid.TryParse(brokered.LockToken, out var guid) ? guid : Guid.NewGuid();

        var header = new Header
        {
            // AMQP Header.DeliveryCount is 0-based (number of prior unsuccessful
            // delivery attempts). The Azure SDK adds 1 to get the 1-based
            // DeliveryCount exposed on ServiceBusReceivedMessage.
            DeliveryCount = (uint)Math.Max(0, brokered.DeliveryCount - 1)
        };

        // Preserve TTL on outgoing messages so the SDK can calculate expiry.
        if (brokered.TimeToLive != TimeSpan.MaxValue && brokered.TimeToLive > TimeSpan.Zero)
        {
            header.Ttl = (uint)brokered.TimeToLive.TotalMilliseconds;
        }

        var properties = new Properties
        {
            MessageId = brokered.MessageId,
            CorrelationId = brokered.CorrelationId,
            ContentType = brokered.ContentType,
            Subject = brokered.Subject,
            ReplyTo = brokered.ReplyTo,
            To = brokered.To,
            GroupId = brokered.SessionId,
            ReplyToGroupId = brokered.ReplyToSessionId,
            CreationTime = brokered.EnqueuedTimeUtc.UtcDateTime
        };

        // Set AbsoluteExpiryTime so the Azure SDK can determine message expiry.
        if (brokered.TimeToLive != TimeSpan.MaxValue && brokered.TimeToLive > TimeSpan.Zero)
        {
            properties.AbsoluteExpiryTime = brokered.EnqueuedTimeUtc.Add(brokered.TimeToLive).UtcDateTime;
        }

        var message = new Message()
        {
            BodySection = new Data { Binary = brokered.Body ?? [] },
            Properties = properties,
            Header = header,
            MessageAnnotations = new MessageAnnotations
            {
                [new Symbol("x-opt-sequence-number")] = brokered.SequenceNumber,
                [new Symbol("x-opt-enqueued-time")] = brokered.EnqueuedTimeUtc.UtcDateTime,
                [new Symbol("x-opt-lock-token")] = lockGuid,
                [new Symbol("x-opt-locked-until")] = brokered.LockedUntil != default
                    ? brokered.LockedUntil.UtcDateTime
                    : DateTimeOffset.UtcNow.Add(TimeSpan.FromMinutes(5)).UtcDateTime
            }
        };

        if (brokered.PartitionKey is not null)
            message.MessageAnnotations[new Symbol("x-opt-partition-key")] = brokered.PartitionKey;

        if (brokered.DeadLetterSource is not null)
            message.MessageAnnotations[new Symbol("x-opt-dead-letter-source")] = brokered.DeadLetterSource;

        if (brokered.ScheduledEnqueueTimeUtc.HasValue)
        {
            message.MessageAnnotations[new Symbol("x-opt-scheduled-enqueue-time")] =
                brokered.ScheduledEnqueueTimeUtc.Value.UtcDateTime;
            // ServiceBusMessageState.Scheduled = 2
            message.MessageAnnotations[new Symbol("x-opt-message-state")] = 2;
        }
        else if (brokered.State == MessageState.Deferred)
        {
            // ServiceBusMessageState.Deferred = 1
            message.MessageAnnotations[new Symbol("x-opt-message-state")] = 1;
        }

        if (brokered.ApplicationProperties.Count > 0
            || brokered.DeadLetterReason is not null
            || brokered.DeadLetterErrorDescription is not null)
        {
            message.ApplicationProperties = new ApplicationProperties();
            foreach (var kvp in brokered.ApplicationProperties)
            {
                message.ApplicationProperties[kvp.Key] = kvp.Value;
            }

            // Azure Service Bus transmits dead-letter metadata as application properties
            if (brokered.DeadLetterReason is not null)
                message.ApplicationProperties["DeadLetterReason"] = brokered.DeadLetterReason;
            if (brokered.DeadLetterErrorDescription is not null)
                message.ApplicationProperties["DeadLetterErrorDescription"] = brokered.DeadLetterErrorDescription;
        }

        return message;
    }

    internal static string? GetLockTokenStatic(Message message)
    {
        if (message.MessageAnnotations?.Map is not null
            && message.MessageAnnotations.Map.TryGetValue(new Symbol("x-opt-lock-token"), out var token))
        {
            return token switch
            {
                Guid g => g.ToString(),
                string s => s,
                _ => token?.ToString()
            };
        }
        return null;
    }

    private static readonly System.Reflection.FieldInfo? CreditField =
        typeof(ListenerLink).GetField("credit", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

    /// <summary>
    /// Reads the link's internal credit counter via reflection.
    /// AMQPNetLite updates this when the client sends Flow frames
    /// (including credit replenishment after completing messages).
    /// </summary>
    internal static uint GetLinkCreditStatic(ListenerLink link) => GetLinkCredit(link);

    private static uint GetLinkCredit(ListenerLink link)
    {
        try { return (uint)(CreditField?.GetValue(link) ?? 0u); }
        catch { return 1u; } // If reflection fails, assume credit available
    }
}
