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
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;

    public ReceiverLinkEndpoint(QueueEntity queue)
    {
        _queue = queue;
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

        try
        {
            if (lockToken is not null && dispositionContext.DeliveryState is not null)
                SettleMessage(lockToken, dispositionContext.DeliveryState);

            dispositionContext.Complete();
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
                // The Azure SDK sends dead-letter reason/description in the Error.Info map.
                // Condition is "com.microsoft:dead-letter", and Info contains the user-specified
                // "DeadLetterReason" and "DeadLetterErrorDescription".
                string? dlReason = rejected.Error?.Condition?.ToString();
                string? dlDescription = rejected.Error?.Description;
                if (rejected.Error?.Info is { } info)
                {
                    // AMQPNetLite deserializes Info map keys as Symbol, not string,
                    // so we iterate and compare via ToString().
                    foreach (var key in info.Keys)
                    {
                        var keyStr = key?.ToString();
                        if (keyStr == "DeadLetterReason" && info[key] is string reason)
                            dlReason = reason;
                        if (keyStr == "DeadLetterErrorDescription" && info[key] is string desc)
                            dlDescription = desc;
                    }
                }
                _queue.DeadLetter(lockToken, dlReason, dlDescription);
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
