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

    public ServiceBusLinkProcessor(NamespaceRegistry registry, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _registry = registry;
        _scheduledProcessor = scheduledProcessor;
    }

    public void Process(AttachContext attachContext)
    {
        // Link.Role == true means the server-side link is a receiver (client is sending)
        // Link.Role == false means the server-side link is a sender (client is receiving)
        var isServerReceiver = attachContext.Link.Role;

        // Note: Transaction coordinator links (Amqp.Transactions.Coordinator targets) are
        // rejected upstream in EmulatorContainer.AttachLink before this processor is called.

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
        address = address?.TrimStart('/');

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
            var endpoint = new SenderLinkEndpoint(context, address, _scheduledProcessor);
            attachContext.Complete(endpoint, 300);
        }
        else
        {
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

            var endpoint = new ReceiverLinkEndpoint(queue);
            attachContext.Complete(endpoint, 0);
        }
    }

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
        Log.LogWarning("HandleSessionReceiver: requested={Requested}, queue={Queue}, receiverId={ReceiverId}",
            requestedSessionId, address, receiverId);
        var session = queue.Sessions.TryAcceptSession(requestedSessionId, receiverId);

        if (session is not null)
        {
            Log.LogWarning("HandleSessionReceiver: ACCEPTED session={SessionId} immediately for receiver={ReceiverId}",
                session.SessionId, receiverId);
            CompleteSessionAttach(attachContext, queue, session);
            return;
        }

        // No session available yet.  Emulate the real Azure Service Bus behavior: hold the
        // AMQP link attach pending and poll until a session becomes available or 65 seconds
        // elapse (at which point a timeout error is sent back to the client).
        // This allows ServiceBusSessionProcessor's concurrent "session pump" tasks to stay
        // alive and pick up sessions as soon as messages arrive.
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(65));

        // Cancel if the client disconnects while we are waiting
        attachContext.Link.Closed += (_, _) => cts.Cancel();

        Task.Run(async () =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    await Task.Delay(100, cts.Token);

                    var accepted = queue.Sessions.TryAcceptSession(requestedSessionId, receiverId);
                    if (accepted is not null)
                    {
                        Log.LogWarning("HandleSessionReceiver: POLL ACCEPTED session={SessionId} for receiver={ReceiverId}",
                            accepted.SessionId, receiverId);
                        CompleteSessionAttach(attachContext, queue, accepted);
                        return;
                    }
                }
            }
            catch (OperationCanceledException) { }

            // Timeout (or client disconnected) — reject with the standard timeout error
            attachContext.Complete(new Error(new Symbol("com.microsoft:timeout"))
            {
                Description = requestedSessionId is not null
                    ? $"Session '{requestedSessionId}' is not available."
                    : "No sessions are available."
            });
        });
    }

    private static void CompleteSessionAttach(AttachContext attachContext, QueueEntity queue, BrokerSessionState session)
    {
        // Create a fresh Properties map — do NOT inherit the client's Attach properties
        // (e.g. com.microsoft:timeout), which would confuse the SDK into thinking the
        // response is a timeout notification rather than a successful session accept.
        attachContext.Attach.Properties = new Fields();
        attachContext.Attach.Properties[new Symbol("com.microsoft:locked-until-utc")] = session.LockedUntil.UtcDateTime;
        attachContext.Attach.Properties[new Symbol("com.microsoft:session-id")] = session.SessionId;

        // The Azure SDK also reads the resolved session ID from the Source filter-set
        // in the attach response. Mirror the filter with the actual session ID so the
        // SDK's AmqpSessionReceiver can populate its SessionId property.
        if (attachContext.Attach.Source is Source src)
        {
            src.FilterSet ??= new Map();
            src.FilterSet[new Symbol("com.microsoft:session-filter")] = session.SessionId;
        }

        var endpoint = new SessionReceiverLinkEndpoint(queue, session);
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
