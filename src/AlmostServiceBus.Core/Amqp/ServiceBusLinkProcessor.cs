using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Logging;
using BrokerSessionState = AlmostServiceBus.Core.Broker.SessionState;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Routes incoming AMQP link attach requests to the appropriate endpoint.
/// </summary>
public class ServiceBusLinkProcessor : ILinkProcessor
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<ServiceBusLinkProcessor>();
    private readonly NamespaceRegistry _registry;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;
    private readonly Broker.Transactions.TransactionManager? _transactions;

    public ServiceBusLinkProcessor(NamespaceRegistry registry, ScheduledMessageProcessor? scheduledProcessor = null, Broker.Transactions.TransactionManager? transactions = null)
    {
        _registry = registry;
        _scheduledProcessor = scheduledProcessor;
        _transactions = transactions;
    }

    public void Process(AttachContext attachContext)
    {
        // Link.Role == true means the server-side link is a receiver (client is sending)
        // Link.Role == false means the server-side link is a sender (client is receiving)
        var isServerReceiver = attachContext.Link.Role;

        // Note: Transaction coordinator links (Amqp.Transactions.Coordinator targets) are
        // handled upstream in EmulatorContainer.AttachLink (attached to a
        // TransactionCoordinatorEndpoint) before this processor is called.

        string? address;
        if (isServerReceiver)
        {
            // Client is sending: address comes from Target
            var target = attachContext.Link.Name; // fallback
            if (attachContext.Attach.Target is Target t)
                address = t.Address;
            else
                address = null;
        }
        else
        {
            // Client is receiving: address comes from Source
            if (attachContext.Attach.Source is Source s)
                address = s.Address;
            else
                address = null;
        }

        // The Azure SDK sends addresses with a leading '/' (e.g. "/my-queue").
        // Trim it to match entity names created via the REST API.
        address = NormaliseAddress(address);

        if (string.IsNullOrEmpty(address))
        {
            attachContext.Complete(new Error(new Symbol("amqp:invalid-field"))
            {
                Description = "Link address is required."
            });
            return;
        }

        // $cbs and $management are handled by EmulatorContainer's request processors
        if (address is "$cbs" or "$management")
        {
            attachContext.Complete(new Error(new Symbol("amqp:not-found"))
            {
                Description = $"Node '{address}' is handled as a request processor, not via link processor."
            });
            return;
        }

        var context = ResolveNamespace(attachContext);

        // Set max message size on the attach frame (256 KB, matching Azure Service Bus standard tier).
        // Without this, the SDK sees -1 and rejects all messages as too large.
        attachContext.Attach.MaxMessageSize = 256 * 1024;

        if (isServerReceiver)
        {
            // Client is sending messages to us -- auto-create entity if needed
            EnsureEntityExists(context, address);
            var endpoint = new SenderLinkEndpoint(context, address, _scheduledProcessor, _transactions);
            attachContext.Complete(endpoint, 300);
        }
        else
        {
            // Cross-entity transactions pin the connection to its first receiver's entity. Real
            // Azure Service Bus rejects a later receiver on a different top-level entity with
            // "Local transactions cannot span multiple top-level entities" — even outside an active
            // transaction (this is what breaks a shared cross-entity client reused to peek/receive
            // across queues). Senders are unaffected: they are transferred "via" the pinned entity.
            if (!CrossEntityTransactionTracker.TryAdmitReceiver(
                    attachContext.Link.Session.Connection, address, out var pinnedEntity))
            {
                attachContext.Complete(new Error(new Symbol("com.microsoft:operation-cancelled"))
                {
                    Description =
                        "Local transactions cannot span multiple top-level entities such as queue or topic. " +
                        $"The connection is pinned to '{pinnedEntity}' because cross-entity transactions are enabled; " +
                        $"a receiver on '{address}' is not allowed. Use a separate client per entity for non-transactional reads."
                });
                return;
            }

            // Check for session filter on receiver link.
            // The Azure SDK sends a com.microsoft:session-filter entry in the filter-set for
            // both AcceptSessionAsync (value = specific session ID) and AcceptNextSessionAsync
            // (value = null or empty string, meaning "accept any available session").
            // AMQPNetLite may deserialize the AMQP null value as either C# null or empty string.
            var sessionFilterKey = new Symbol("com.microsoft:session-filter");
            string? requestedSessionId = null;
            bool hasSessionFilter = false;

            if (attachContext.Attach.Source is Source src && src.FilterSet is Map filterMap)
            {
                if (filterMap.ContainsKey(sessionFilterKey))
                {
                    var raw = filterMap[sessionFilterKey];
                    hasSessionFilter = true;
                    // Null or empty string both mean "accept next available session".
                    requestedSessionId = raw switch
                    {
                        string s when string.IsNullOrEmpty(s) => null,
                        string s => s,
                        DescribedValue dv when string.IsNullOrEmpty(dv.Value as string) => null,
                        DescribedValue dv => dv.Value as string,
                        _ => null,
                    };
                }
                else
                {
                    // Also check if any filter value is a DescribedValue whose descriptor
                    // matches the session filter symbol (some serializers wrap the entry).
                    foreach (var kvp in filterMap)
                    {
                        if (kvp.Value is DescribedValue dv && dv.Descriptor is Symbol sym && (string)sym == "com.microsoft:session-filter")
                        {
                            hasSessionFilter = true;
                            requestedSessionId = string.IsNullOrEmpty(dv.Value as string) ? null : dv.Value as string;
                            break;
                        }
                    }
                }
            }

            if (hasSessionFilter)
            {
                HandleSessionReceiver(attachContext, context, address, requestedSessionId);
                return;
            }

            // Client is receiving messages from us -- resolve queue
            var queue = context.ResolveQueue(address);
            if (queue is null)
            {
                attachContext.Complete(new Error(new Symbol("amqp:not-found"))
                {
                    Description = $"Queue or subscription '{address}' not found."
                });
                return;
            }

            // Reject non-session receiver attaches against session-required queues — real
            // Service Bus returns an error and the SDK propagates this as
            // InvalidOperationException ("entity does not allow non-session receiver").
            // Only reject the regular receiver; session receivers come through HandleSessionReceiver.
            if (queue.RequiresSession)
            {
                attachContext.Complete(new Error(new Symbol("com.microsoft:session-required"))
                {
                    Description = $"The entity '{address}' requires session-aware receivers."
                });
                return;
            }

            // ReceiveAndDelete mode: client sets SndSettleMode=Settled (pre-settled, value=1).
            // The broker should settle messages on send and not expect a disposition.
            // SndSettleMode is a byte: 0=Unsettled, 1=Settled, 2=Mixed.
            var preSettled = (byte)attachContext.Attach.SndSettleMode == 1;
            var endpoint = new ReceiverLinkEndpoint(queue, preSettled, _transactions);
            attachContext.Complete(endpoint, 0);
        }
    }

    /// <summary>
    /// Upper bound on how long a next-available-session attach is held pending when the client
    /// does not say how long it is prepared to wait. Matches the cap real Service Bus applies.
    /// </summary>
    internal static readonly TimeSpan MaxSessionAcceptWait = TimeSpan.FromSeconds(65);

    private static readonly Symbol ClientTimeoutProperty = new("com.microsoft:timeout");

    private void HandleSessionReceiver(AttachContext attachContext, NamespaceContext ns, string address, string? requestedSessionId)
    {
        var queue = ns.ResolveQueue(address);
        if (queue is null || !queue.RequiresSession || queue.Sessions is null)
        {
            attachContext.Complete(new Error(new Symbol("amqp:not-found"))
            {
                Description = $"Session-enabled queue '{address}' not found."
            });
            return;
        }

        // Set max message size
        attachContext.Attach.MaxMessageSize = 256 * 1024;

        var receiverId = attachContext.Link.Name ?? Guid.NewGuid().ToString();
        Log.LogDebug("HandleSessionReceiver: requested={Requested}, queue={Queue}, receiverId={ReceiverId}",
            requestedSessionId, address, receiverId);
        var session = queue.Sessions.TryAcceptSession(requestedSessionId, receiverId);

        if (session is not null)
        {
            Log.LogDebug("HandleSessionReceiver: ACCEPTED session={SessionId} immediately for receiver={ReceiverId}",
                session.SessionId, receiverId);
            CompleteSessionAttach(attachContext, queue, session);
            return;
        }

        // If the client requested a SPECIFIC session that is already locked, reject
        // immediately with com.microsoft:session-cannot-be-locked. Real Azure Service Bus
        // does this — it does not hold the attach pending while another receiver holds the
        // session lock. The SDK maps this error to ServiceBusFailureReason.SessionCannotBeLocked.
        if (!string.IsNullOrEmpty(requestedSessionId)
            && queue.Sessions.IsSessionLocked(requestedSessionId))
        {
            attachContext.Complete(new Error(new Symbol("com.microsoft:session-cannot-be-locked"))
            {
                Description = $"Session '{requestedSessionId}' is locked by another receiver."
            });
            return;
        }

        // No session available yet. Emulate real Service Bus: hold the attach pending and poll
        // until a session becomes available or the wait expires, then answer with a timeout error.
        // This is what keeps ServiceBusSessionProcessor's concurrent "accept next session" tasks
        // alive so they pick up sessions as soon as messages arrive.
        var wait = ResolveSessionAcceptWait(attachContext.Attach);
        _ = new PendingSessionAttach(attachContext, queue, receiverId, requestedSessionId, wait, CompleteSessionAttach).RunAsync();
    }

    /// <summary>
    /// How long to hold a pending session attach before answering with <c>com.microsoft:timeout</c>.
    /// </summary>
    /// <remarks>
    /// For next-available-session the Azure SDK puts its operation timeout (minus a small buffer) in
    /// the Attach's <c>com.microsoft:timeout</c> property precisely so the <em>service</em> gives up
    /// first. The client itself abandons the attach at its full timeout, closes the link and ends
    /// the AMQP session. If we wait longer than the client — the old fixed 65s against the SDK's
    /// 60s default did exactly that — our eventual Attach lands on a session channel the client has
    /// already removed, and Microsoft.Azure.Amqp tears down the whole connection with
    /// "The session channel 'N' cannot be found". Every link on that connection dies with it, which
    /// is what showed up as duplicate deliveries and transport faults across unrelated queues
    /// under load. Real Service Bus caps the wait at 65 seconds; so do we.
    /// </remarks>
    internal static TimeSpan ResolveSessionAcceptWait(Attach attach)
    {
        if (attach.Properties is { } props && props.TryGetValue(ClientTimeoutProperty, out var raw))
        {
            double? millis = raw switch
            {
                uint u => u,
                int i => i,
                long l => l,
                ulong ul => ul,
                _ => null,
            };

            if (millis is > 0)
            {
                var requested = TimeSpan.FromMilliseconds(millis.Value);
                return requested < MaxSessionAcceptWait ? requested : MaxSessionAcceptWait;
            }
        }

        return MaxSessionAcceptWait;
    }

    /// <summary>
    /// A next-available-session (or not-yet-populated specific session) attach that is being held
    /// open until a session can be locked for it. Every frame we might send on the pending link —
    /// the accepting Attach or the timeout error — is gated on the link still being open, and the
    /// gate is taken under a lock so a client Detach/End racing the accept cannot slip through.
    /// </summary>
    private sealed class PendingSessionAttach
    {
        private readonly AttachContext _attachContext;
        private readonly QueueEntity _queue;
        private readonly string _receiverId;
        private readonly string? _requestedSessionId;
        private readonly TimeSpan _wait;
        private readonly Action<AttachContext, QueueEntity, BrokerSessionState> _complete;
        private readonly CancellationTokenSource _cts = new();
        private readonly Lock _gate = new();
        private bool _linkClosed;
        private bool _completed;

        public PendingSessionAttach(
            AttachContext attachContext,
            QueueEntity queue,
            string receiverId,
            string? requestedSessionId,
            TimeSpan wait,
            Action<AttachContext, QueueEntity, BrokerSessionState> complete)
        {
            _attachContext = attachContext;
            _queue = queue;
            _receiverId = receiverId;
            _requestedSessionId = requestedSessionId;
            _wait = wait;
            _complete = complete;
        }

        public async Task RunAsync()
        {
            // AddClosedCallback (rather than the Closed event) fires immediately if the link is
            // already closed, so a client that gave up before we got here is caught too. The link
            // is closed for a plain Detach, for the client ending the AMQP session, and for the
            // connection going away — AMQPNetLite aborts every link in all three cases.
            _attachContext.Link.AddClosedCallback((_, _) =>
            {
                lock (_gate)
                {
                    _linkClosed = true;
                }
                _cts.Cancel();
            });

            var timedOut = false;
            try
            {
                using var timeout = new CancellationTokenSource(_wait);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, timeout.Token);

                try
                {
                    while (true)
                    {
                        await Task.Delay(100, linked.Token);

                        var accepted = _queue.Sessions!.TryAcceptSession(_requestedSessionId, _receiverId);
                        if (accepted is null)
                            continue;

                        if (TryCompleteWithSession(accepted))
                            return;

                        // The link closed between the poll and the accept: give the lock straight
                        // back so another receiver can take the session immediately.
                        _queue.Sessions.ReleaseSession(accepted.SessionId);
                        Log.LogDebug("HandleSessionReceiver: client went away after accepting session={SessionId}, released lock",
                            accepted.SessionId);
                        return;
                    }
                }
                catch (OperationCanceledException)
                {
                    timedOut = timeout.IsCancellationRequested && !_cts.IsCancellationRequested;
                }
            }
            catch (Exception ex)
            {
                Log.LogWarning(ex, "HandleSessionReceiver: polling loop failed for queue={Queue}, receiverId={ReceiverId}",
                    _queue.Name, _receiverId);
            }

            if (!timedOut)
            {
                // The client detached, ended the session or dropped the connection while we were
                // waiting. Say nothing: any frame we sent now would target a channel it no longer has.
                Log.LogDebug("HandleSessionReceiver: link closed while waiting for a session on queue={Queue}, receiverId={ReceiverId}",
                    _queue.Name, _receiverId);
                return;
            }

            TryCompleteWithTimeout();
        }

        private bool TryCompleteWithSession(BrokerSessionState session)
        {
            lock (_gate)
            {
                if (_linkClosed || _completed)
                    return false;

                try
                {
                    Log.LogDebug("HandleSessionReceiver: POLL ACCEPTED session={SessionId} for receiver={ReceiverId}",
                        session.SessionId, _receiverId);
                    _complete(_attachContext, _queue, session);
                    _completed = true;
                    return true;
                }
                catch (Exception ex)
                {
                    // CompleteAttach failed — the link is unusable. Report "not completed" so the
                    // caller releases the session lock instead of leaving it held for LockDuration.
                    Log.LogWarning(ex,
                        "HandleSessionReceiver: CompleteSessionAttach failed for session={SessionId}",
                        session.SessionId);
                    _completed = true;
                    return false;
                }
            }
        }

        private void TryCompleteWithTimeout()
        {
            lock (_gate)
            {
                if (_linkClosed || _completed)
                    return;
                _completed = true;

                try
                {
                    _attachContext.Complete(new Error(new Symbol("com.microsoft:timeout"))
                    {
                        Description = _requestedSessionId is not null
                            ? $"Session '{_requestedSessionId}' is not available."
                            : "No sessions are available."
                    });
                }
                catch (Exception ex)
                {
                    Log.LogDebug(ex, "HandleSessionReceiver: failed to send timeout error (link likely already closed)");
                }
            }
        }
    }

    private void CompleteSessionAttach(AttachContext attachContext, QueueEntity queue, BrokerSessionState session)
    {
        // Reclaim any pending messages orphaned by a previous receiver of this session.
        // Because we just locked the session (via TryAcceptSession), any pending messages
        // in _queue._pending for this session must be from a previous (dead) receiver —
        // we haven't dequeued anything yet on this new link. Re-enqueuing them back to
        // the session queue lets us process them in order.
        //
        // This replaces the OnLinkClosed-based reclaim, which caused R-DUPE cascades by
        // racing with in-flight settlements from the closing receiver.
        try
        {
            queue.ReclaimPendingForSession(session.SessionId);
        }
        catch (Exception ex)
        {
            Log.LogDebug(ex, "ReclaimPendingForSession failed for session '{SessionId}'", session.SessionId);
        }

        // Create a fresh Properties map — do NOT inherit the client's Attach properties
        // (e.g. com.microsoft:timeout), which would confuse the SDK into thinking the
        // response is a timeout notification rather than a successful session accept.
        //
        // CRITICAL: The Azure SDK reads com.microsoft:locked-until-utc as a `long`
        // (UTC ticks) — not a DateTime. See AmqpReceiver.OpenReceiverLinkAsync in
        // azure-sdk-for-net. Sending DateTime causes TryGetValue<long> to fail and
        // SessionLockedUntil becomes DateTime.MinValue on the SDK side.
        attachContext.Attach.Properties = new Fields();
        attachContext.Attach.Properties[new Symbol("com.microsoft:locked-until-utc")] = session.LockedUntil.UtcTicks;
        attachContext.Attach.Properties[new Symbol("com.microsoft:session-id")] = session.SessionId;

        // The Azure SDK also reads the resolved session ID from the Source filter-set
        // in the attach response. Mirror the filter with the actual session ID so the
        // SDK's AmqpSessionReceiver can populate its SessionId property.
        if (attachContext.Attach.Source is Source src)
        {
            src.FilterSet ??= new Map();
            src.FilterSet[new Symbol("com.microsoft:session-filter")] = session.SessionId;
        }

        // Pre-settled mode (ReceiveAndDelete) — same logic as the regular receiver.
        var preSettled = (byte)attachContext.Attach.SndSettleMode == 1;
        var endpoint = new SessionReceiverLinkEndpoint(queue, session, preSettled, _transactions);
        attachContext.Complete(endpoint, 0);
    }

    /// <summary>
    /// Resolves the namespace from the AMQP connection.
    /// First checks for a namespace stored by CBS authentication (from SharedAccessKeyName).
    /// Falls back to the connection's OPEN frame hostname, then "default".
    /// </summary>
    private NamespaceContext ResolveNamespace(AttachContext attachContext)
    {
        var connection = attachContext.Link.Session.Connection;

        // 1. Check if CBS auth stored a namespace from SharedAccessKeyName
        var keyName = CbsRequestProcessor.GetNamespaceForConnection(connection);
        if (keyName is not null)
        {
            return _registry.GetOrCreate(keyName);
        }

        // 2. Fall back to hostname from OPEN frame
        try
        {
            var openProp = connection.GetType().GetProperty("Open",
                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (openProp?.GetValue(connection) is Open open && !string.IsNullOrEmpty(open.HostName))
            {
                var host = open.HostName;
                var namespaceName = host.Split('.')[0];
                if (!namespaceName.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                    return _registry.GetOrCreate(namespaceName);
            }
        }
        catch { }

        // 3. Default
        return _registry.GetOrCreate("default");
    }

    /// <summary>
    /// Reduces a link address to the entity path.
    /// </summary>
    /// <remarks>
    /// The .NET client sends the entity path, with or without a leading '/'. The Python client sends
    /// the whole address it connected to, such as
    /// "amqps://localhost:5673/my-topic/Subscriptions/my-subscription". Without this, a Python
    /// receiver is answered "not found", and a Python sender is worse off still: the send target is
    /// created on demand, so the URI became a queue of its own and every published message went into
    /// it instead of the topic, with no error on either side.
    /// </remarks>
    internal static string? NormaliseAddress(string? address)
    {
        if (string.IsNullOrEmpty(address))
            return address;

        var schemeEnd = address.IndexOf("://", StringComparison.Ordinal);

        if (schemeEnd >= 0)
        {
            var pathStart = address.IndexOf('/', schemeEnd + 3);
            address = pathStart >= 0 ? address[pathStart..] : string.Empty;
        }

        return address.TrimStart('/');
    }

    private static void EnsureEntityExists(NamespaceContext context, string address)
    {
        // If neither a queue nor topic exists for this address, create a queue
        var (queue, topic) = context.ResolveSendTarget(address);
        if (queue is null && topic is null)
        {
            context.CreateQueue(address);
        }
    }
}
