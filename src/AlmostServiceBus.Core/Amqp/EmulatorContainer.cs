using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Custom <see cref="IContainer"/> implementation that replaces AMQPNetLite's
/// <see cref="ContainerHost"/>. This fixes a crash in ContainerHost.AttachLink
/// when the client sends an Attach frame with a <see cref="global::Amqp.Transactions.Coordinator"/>
/// target (used for AMQP transactions by NServiceBus). ContainerHost blindly casts
/// attach.Target to <see cref="Target"/>, which throws an InvalidCastException.
///
/// By implementing IContainer ourselves, we can intercept Coordinator targets
/// and detach the link gracefully before the cast occurs.
/// </summary>
public class EmulatorContainer : IContainer
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<EmulatorContainer>();

    private readonly Dictionary<string, RequestProcessorEntry> _requestProcessors = new(StringComparer.OrdinalIgnoreCase);
    private ILinkProcessor? _linkProcessor;
    private NamespaceRegistry? _registry;
    private ScheduledMessageProcessor? _scheduledProcessor;

    // Tracks sender link names → entity paths so that com.microsoft:schedule-message
    // can resolve the target entity from the "associated-link-name" (which is typically
    // the AMQP sender link name, not the entity path). Link names use case-insensitive
    // comparison because the Azure SDK may vary casing across SDK versions.
    public readonly record struct SenderLinkTarget(string Address, string NamespaceName);

    private readonly ConcurrentDictionary<string, SenderLinkTarget> _senderLinkNames = new(StringComparer.OrdinalIgnoreCase);

    // Reflection accessor for AttachContext's internal constructor.
    // AttachContext(ListenerLink link, Attach attach) is internal in AMQPNetLite,
    // so we must use reflection to create instances for the ILinkProcessor.
    private static readonly ConstructorInfo? AttachContextCtor =
        typeof(AttachContext).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [typeof(ListenerLink), typeof(Attach)],
            null);

    // Reflection accessor for RequestContext's internal constructor.
    // RequestContext(ListenerLink requestLink, ListenerLink responseLink, Message request) is internal.
    private static readonly ConstructorInfo? RequestContextCtor =
        typeof(RequestContext).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [typeof(ListenerLink), typeof(ListenerLink), typeof(Message)],
            null);

    // Reflection accessor for ListenerLink.SettleOnSend which has an internal setter.
    private static readonly PropertyInfo? SettleOnSendProperty =
        typeof(ListenerLink).GetProperty("SettleOnSend");


    public X509Certificate2? ServiceCertificate => null;

    public IDictionary<string, TransportProvider> CustomTransports { get; } = new Dictionary<string, TransportProvider>();

    /// <summary>
    /// Configures the namespace registry and scheduled processor for entity-scoped management links.
    /// </summary>
    public void SetNamespaceRegistry(NamespaceRegistry registry, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _registry = registry;
        _scheduledProcessor = scheduledProcessor;
    }

    /// <summary>
    /// Creates a <see cref="ManagementLinkEndpoint"/> that has access to the sender link name registry,
    /// allowing it to resolve entity paths from AMQP link names in schedule-message requests.
    /// </summary>
    public ManagementLinkEndpoint CreateManagementEndpoint(NamespaceContext context, ScheduledMessageProcessor? scheduledProcessor = null, QueueEntity? scopedQueue = null)
    {
        return new ManagementLinkEndpoint(context, scheduledProcessor, scopedAddress: scopedQueue?.Name, scopedQueue: scopedQueue, senderLinkNames: _senderLinkNames, registry: _registry);
    }

    /// <summary>
    /// Registers an <see cref="IRequestProcessor"/> for a given address (e.g. "$cbs", "$management").
    /// </summary>
    public void RegisterRequestProcessor(string address, IRequestProcessor processor)
    {
        lock (_requestProcessors)
        {
            _requestProcessors[address] = new RequestProcessorEntry(processor);
        }
    }

    /// <summary>
    /// Registers the fallback <see cref="ILinkProcessor"/> for links that don't match
    /// any request processor address.
    /// </summary>
    public void RegisterLinkProcessor(ILinkProcessor linkProcessor)
    {
        _linkProcessor = linkProcessor;
    }

    public Message CreateMessage(ByteBuffer buffer)
    {
        return Message.Decode(buffer);
    }

    private readonly ConcurrentDictionary<string, byte> _trackedConnections = new();

    public Link CreateLink(ListenerConnection connection, ListenerSession session, Attach attach)
    {
        // Track connection lifecycle — log when connections close with errors
        var connId = connection.GetHashCode().ToString();
        if (_trackedConnections.TryAdd(connId, 0))
        {
            ((Connection)connection).Closed += (sender, error) =>
            {
                _trackedConnections.TryRemove(connId, out _);
                if (error != null)
                    Log.LogWarning("AMQP connection closed with error: {ConnectionId} — {Error}", connId, error);
            };
        }

        return new ListenerLink(session, attach);
    }

    public bool AttachLink(ListenerConnection connection, ListenerSession session, Link link, Attach attach)
    {
        var listenerLink = (ListenerLink)link;

        // Reject transaction coordinator links. This is the whole reason we replaced ContainerHost:
        // ContainerHost.AttachLink does ((Target)attach.Target).Address which throws InvalidCastException
        // when attach.Target is Coordinator.
        if (attach.Target is global::Amqp.Transactions.Coordinator)
        {
            Log.LogInformation("Rejecting transaction coordinator link '{LinkName}' — transactions not supported.", attach.LinkName);
            listenerLink.CompleteAttach(attach, new Error(new Symbol("amqp:not-implemented"))
            {
                Description = "AMQP transactions are not supported by the emulator."
            });
            return false;
        }

        // Resolve the address from the attach frame, matching ContainerHost's behavior:
        //   address = attach.Role ? ((Source)attach.Source).Address : ((Target)attach.Target).Address
        //
        // attach.Role == true  → remote is *receiver* → address on Source
        // attach.Role == false → remote is *sender*   → address on Target
        string? address = null;
        if (attach.Role)
        {
            if (attach.Source is Source s)
                address = s.Address;
        }
        else
        {
            if (attach.Target is Target t)
                address = t.Address;
        }

        // Diagnostic logging for link attachment
        var sourceAddr = (attach.Source as Source)?.Address ?? "(null)";
        var targetAddr = (attach.Target as Target)?.Address ?? "(null)";
        var isDynamic = (attach.Source as Source)?.Dynamic ?? false;
        Log.LogDebug(
            "AttachLink: Role={Role}, ResolvedAddress={Address}, Source.Address={SourceAddr}, Target.Address={TargetAddr}, Dynamic={Dynamic}, LinkName={LinkName}",
            attach.Role, address ?? "(null)", sourceAddr, targetAddr, isDynamic, attach.LinkName);

        // Check if a request processor is registered for this address.
        if (address != null)
        {
            RequestProcessorEntry? entry;
            lock (_requestProcessors)
            {
                var processorKey = address;
                if (address.EndsWith("/$management", StringComparison.OrdinalIgnoreCase))
                {
                    processorKey = BuildEntityManagementProcessorKey(listenerLink.Session.Connection, address);
                }

                _requestProcessors.TryGetValue(processorKey, out entry);

                // Entity-scoped $management links (e.g. "my-queue/$management")
                // Create an entity-specific management endpoint if possible, otherwise
                // fall back to the global $management request processor.
                if (entry is null && address.EndsWith("/$management", StringComparison.OrdinalIgnoreCase))
                {
                    var entityName = address[..^"/$management".Length].TrimStart('/');
                    var entityEntry = TryCreateEntityManagementEntry(entityName, ResolveNamespace(listenerLink.Session.Connection));
                    if (entityEntry is not null)
                    {
                        _requestProcessors[processorKey] = entityEntry;
                        entry = entityEntry;
                        Log.LogDebug("AttachLink REQUEST: created entity management entry for '{Address}', key='{Key}'", address, processorKey);
                    }
                    else
                    {
                        _requestProcessors.TryGetValue("$management", out entry);
                        Log.LogDebug("AttachLink REQUEST: fell back to global $management for '{Address}', key='{Key}'", address, processorKey);
                    }
                }
                else if (entry is not null && address.EndsWith("/$management", StringComparison.OrdinalIgnoreCase))
                {
                    Log.LogDebug("AttachLink REQUEST: reused existing entry for '{Address}', key='{Key}'", address, processorKey);
                }
            }

            if (entry != null)
            {
                AttachRequestProcessorLink(entry, listenerLink, address, attach);
                return true;
            }
        }

        // For response links (Role=false / server is sender), the Target.Address is the client's
        // dynamic reply-to address (e.g. "client$abc123"), not the management address.
        // Check Source.Address to find the management entry this response link belongs to.
        // The SDK attaches response links with Source.Address = "myqueue/$management".
        if (!attach.Role && attach.Source is Source responseSource && responseSource.Address != null)
        {
            var sourceAddress = responseSource.Address;
            RequestProcessorEntry? entry;
            lock (_requestProcessors)
            {
                var processorKey = sourceAddress;
                if (sourceAddress.EndsWith("/$management", StringComparison.OrdinalIgnoreCase))
                {
                    processorKey = BuildEntityManagementProcessorKey(listenerLink.Session.Connection, sourceAddress);
                }

                _requestProcessors.TryGetValue(processorKey, out entry);

                if (entry is null && sourceAddress.EndsWith("/$management", StringComparison.OrdinalIgnoreCase))
                {
                    var entityName = sourceAddress[..^"/$management".Length].TrimStart('/');
                    var entityEntry = TryCreateEntityManagementEntry(entityName, ResolveNamespace(listenerLink.Session.Connection));
                    if (entityEntry is not null)
                    {
                        _requestProcessors[processorKey] = entityEntry;
                        entry = entityEntry;
                        Log.LogDebug("AttachLink RESPONSE: created NEW entity management entry for '{Address}', key='{Key}' — request link may be on a different entry!", sourceAddress, processorKey);
                    }
                    else
                    {
                        _requestProcessors.TryGetValue("$management", out entry);
                        Log.LogDebug("AttachLink RESPONSE: fell back to global $management for '{Address}', key='{Key}'", sourceAddress, processorKey);
                    }
                }
                else if (entry is not null)
                {
                    Log.LogDebug("AttachLink RESPONSE: reused existing entry for '{Address}', key='{Key}'", sourceAddress, processorKey);
                }
                else
                {
                    Log.LogDebug("AttachLink RESPONSE: no entry found for '{Address}', key='{Key}'", sourceAddress, processorKey);
                }
            }

            if (entry != null)
            {
                AttachRequestProcessorLink(entry, listenerLink, sourceAddress, attach);
                return true;
            }
        }

        // Fall back to the link processor for all other links.
        // Track sender link names → entity paths so that schedule-message can resolve
        // the target entity from "associated-link-name" (which is the sender link name).
        // Only track non-management sender links (role=false = remote is sender, server receives).
        if (!attach.Role && address != null
            && !address.EndsWith("/$management", StringComparison.OrdinalIgnoreCase)
            && !address.Equals("$management", StringComparison.OrdinalIgnoreCase)
            && !address.Equals("$cbs", StringComparison.OrdinalIgnoreCase))
        {
            var target = new SenderLinkTarget(address, ResolveNamespace(listenerLink.Session.Connection));
            foreach (var key in BuildSenderLinkRegistryKeys(listenerLink.Session.Connection, attach.LinkName))
                _senderLinkNames[key] = target;
        }

        if (_linkProcessor != null)
        {
            var attachContext = CreateAttachContext(listenerLink, attach);
            if (attachContext != null)
            {
                _linkProcessor.Process(attachContext);
            }
            else
            {
                // Reflection failed — complete the attach with an error.
                Log.LogError("Failed to create AttachContext via reflection. Cannot dispatch link '{LinkName}'.", attach.LinkName);
                listenerLink.CompleteAttach(attach, new Error(new Symbol("amqp:internal-error"))
                {
                    Description = "Internal error creating link context."
                });
            }

            // Return false because the link processor completes the attach asynchronously.
            return false;
        }

        // No processor found for this address.
        if (string.IsNullOrWhiteSpace(address))
        {
            listenerLink.CompleteAttach(attach, new Error(new Symbol("amqp:invalid-field"))
            {
                Description = "The address field cannot be empty."
            });
        }
        else
        {
            listenerLink.CompleteAttach(attach, new Error(new Symbol("amqp:not-found"))
            {
                Description = $"No processor was found at {address}"
            });
        }

        return false;
    }

    /// <summary>
    /// Attaches a link to a request processor, replicating ContainerHost's internal RequestProcessor.AddLink behavior.
    /// Request processors use a request-response pattern: a receiver link (incoming requests) and a sender link (outgoing responses).
    /// </summary>
    private static void AttachRequestProcessorLink(RequestProcessorEntry entry, ListenerLink link, string address, Attach attach)
    {
        if (!link.Role)
        {
            // This is the response link (server sends responses back to client).
            // The client's attach has a Target with the reply-to address.
            if (attach.Target is not Target target)
            {
                Log.LogWarning("Response link for '{Address}' has no Target — cannot extract reply-to. Target type: {TargetType}",
                    address, attach.Target?.GetType().FullName ?? "null");
                link.CompleteAttach(attach, new Error(new Symbol("amqp:internal-error"))
                {
                    Description = $"Response link for '{address}' has no Target."
                });
                return;
            }
            // The reply-to address identifies the response link for request correlation.
            // Microsoft.Azure.Amqp sets Target.Address to a GUID used as ReplyTo in requests.
            // Fall back to the link name if Target.Address is unexpectedly null.
            var replyTo = target.Address ?? link.Name;

            lock (entry.ResponseLinks)
            {
                entry.ResponseLinks[replyTo] = link;
            }

            Log.LogDebug("AttachRequestProcessorLink: response link for '{Address}', replyTo='{ReplyTo}'", address, replyTo);

            // SettleOnSend has an internal setter — use reflection.
            SettleOnSendProperty?.SetValue(link, true);
            link.InitializeSender(
                onCredit: (c, p, s) => { },
                onDispose: (msg, state, settled, s) => { },
                state: Tuple.Create(entry, replyTo));

            link.Closed += (sender, error) =>
            {
                if (sender is ListenerLink closedLink)
                {
                    var tuple = (Tuple<RequestProcessorEntry, string>)closedLink.State;
                    lock (tuple.Item1.ResponseLinks)
                    {
                        tuple.Item1.ResponseLinks.Remove(tuple.Item2);
                    }
                }
            };

            // Do NOT call CompleteAttach here — AttachLink returns true for request processors,
            // so ListenerLink.OnAttach will call CompleteAttach automatically.
        }
        else
        {
            // This is the request link (server receives requests from client).
            var processor = entry.Processor;

            link.InitializeReceiver(
                (uint)processor.Credit,
                (receiverLink, message, deliveryState, state) =>
                {
                    var rp = (RequestProcessorEntry)state;
                    DispatchRequest(receiverLink, message, rp);
                },
                entry);

            link.Closed += (sender, error) =>
            {
                if (sender is ListenerLink closedLink)
                {
                    var rp = (RequestProcessorEntry)closedLink.State;
                    lock (rp.RequestLinks)
                    {
                        rp.RequestLinks.Remove(closedLink);
                    }
                }
            };

            lock (entry.RequestLinks)
            {
                entry.RequestLinks.Add(link);
            }

            // Do NOT call CompleteAttach here — AttachLink returns true for request processors,
            // so ListenerLink.OnAttach will call CompleteAttach automatically.
        }
    }

    /// <summary>
    /// Dispatches a received request message to the IRequestProcessor, replicating
    /// ContainerHost's internal RequestProcessor.DispatchRequest behavior.
    /// </summary>
    private static void DispatchRequest(ListenerLink link, Message message, RequestProcessorEntry entry)
    {
        var operation = message.ApplicationProperties?["operation"] as string;

        // Find the response link for this request.
        ListenerLink? responseLink = null;
        if (message.Properties?.ReplyTo != null)
        {
            lock (entry.ResponseLinks)
            {
                Log.LogDebug("DispatchRequest: operation={Operation}, ReplyTo={ReplyTo}, ResponseLinkCount={Count}",
                    operation, message.Properties?.ReplyTo, entry.ResponseLinks.Count);
                entry.ResponseLinks.TryGetValue(message.Properties!.ReplyTo, out responseLink);
                Log.LogDebug(
                    "DispatchRequest: ReplyTo={ReplyTo}, ResponseLinkKeys=[{Keys}], Found={Found}",
                    message.Properties.ReplyTo,
                    string.Join(", ", entry.ResponseLinks.Keys),
                    responseLink != null);
            }
        }
        else
        {
            Log.LogDebug("DispatchRequest: operation={Operation}, message.Properties.ReplyTo is null", operation);
        }

        if (responseLink == null)
        {
            // Strategy 1: find a response link on the SAME connection as the request link.
            // When multiple connections share a single request processor (e.g. $cbs),
            // each connection has its own response link. The ReplyTo may not match the
            // stored Target.Address, but we can match by connection identity.
            lock (entry.ResponseLinks)
            {
                foreach (var rl in entry.ResponseLinks.Values)
                {
                    try
                    {
                        if (rl.Session.Connection == link.Session.Connection)
                        {
                            responseLink = rl;
                            Log.LogDebug("DispatchRequest: using connection-matched response link");
                            break;
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        // Session/connection may be closing — skip this link.
                    }
                }
            }
        }

        if (responseLink == null)
        {
            // Strategy 2: if ReplyTo didn't match but there's exactly one response link,
            // use it. This handles cases where the SDK's ReplyTo format doesn't match
            // the Target.Address stored during link attachment.
            lock (entry.ResponseLinks)
            {
                if (entry.ResponseLinks.Count == 1)
                {
                    responseLink = entry.ResponseLinks.Values.First();
                    Log.LogDebug("DispatchRequest: using fallback response link (single available)");
                }
            }
        }

        if (responseLink == null)
        {
            // No response link — reject the message.
            Log.LogWarning("DispatchRequest: no response link found for ReplyTo={ReplyTo}", message.Properties?.ReplyTo);
            link.DisposeMessage(message, new Rejected
            {
                Error = new Error(new Symbol("amqp:not-found"))
                {
                    Description = "No response link was found. Ensure the link is attached or reply-to is set on the request."
                }
            }, true);
            return;
        }

        // Accept the request.
        link.DisposeMessage(message, new Accepted(), true);

        // Create RequestContext via reflection (internal constructor).
        var context = CreateRequestContext(link, responseLink, message);
        if (context != null)
        {
            entry.Processor.Process(context);
        }
        else
        {
            Log.LogError("Failed to create RequestContext via reflection.");
        }
    }

    /// <summary>
    /// Attempts to create an entity-specific management endpoint for the given entity name.
    /// Returns null if the registry is not configured or the entity is not found.
    /// </summary>
    private RequestProcessorEntry? TryCreateEntityManagementEntry(string entityName, string? preferredNamespaceName = null)
    {
        if (_registry is null)
        {
            Log.LogWarning("TryCreateEntityManagementEntry: registry is null for entity '{EntityName}'", entityName);
            return null;
        }

        NamespaceContext? context = null;
        QueueEntity? queue = null;
        TopicEntity? topic = null;

        if (!string.IsNullOrWhiteSpace(preferredNamespaceName))
        {
            context = _registry.Get(preferredNamespaceName!);
            queue = context?.ResolveQueue(entityName);
            topic = context?.GetTopic(entityName);
        }

        if (queue is null && topic is null)
        {
            foreach (var nsName in _registry.ListNamespaces())
            {
                var ns = _registry.Get(nsName);
                if (ns is null) continue;
                queue = ns.ResolveQueue(entityName);
                topic = ns.GetTopic(entityName);
                if (queue is not null || topic is not null) { context = ns; break; }
            }
        }

        if (context is null)
            context = _registry.GetOrCreate("default");
        if (queue is null && topic is null)
        {
            queue = context.ResolveQueue(entityName);
            topic = context.GetTopic(entityName);
        }
        if (queue is not null || topic is not null)
        {
            Log.LogDebug("TryCreateEntityManagementEntry: Created entry for entity '{EntityName}', HasSessions={HasSessions}, IsTopic={IsTopic}",
                entityName, queue?.Sessions is not null, topic is not null);
            var processor = new ManagementLinkEndpoint(context, _scheduledProcessor, scopedAddress: entityName, scopedQueue: queue, senderLinkNames: _senderLinkNames, registry: _registry);
            return new RequestProcessorEntry(processor);
        }

        Log.LogWarning("TryCreateEntityManagementEntry: Entity not found for '{EntityName}'", entityName);
        return null;
    }

    private static string BuildEntityManagementProcessorKey(Connection connection, string address)
    {
        return $"{ResolveNamespace(connection)}|{address}";
    }

    internal static string BuildSenderLinkRegistryKey(Connection connection, string linkName)
    {
        var identityKey = CbsRequestProcessor.GetConnectionIdentityKey(connection);
        if (!string.IsNullOrWhiteSpace(identityKey))
            return $"{identityKey}|{linkName}";

        return BuildNamespaceScopedSenderLinkRegistryKey(connection, linkName);
    }

    internal static string BuildNamespaceScopedSenderLinkRegistryKey(Connection connection, string linkName)
    {
        return $"{ResolveNamespace(connection)}|{linkName}";
    }

    internal static IEnumerable<string> BuildSenderLinkRegistryKeys(Connection connection, string linkName)
    {
        var primaryKey = BuildSenderLinkRegistryKey(connection, linkName);
        yield return primaryKey;

        var namespaceKey = BuildNamespaceScopedSenderLinkRegistryKey(connection, linkName);
        if (!namespaceKey.Equals(primaryKey, StringComparison.Ordinal))
            yield return namespaceKey;
    }

    private static string ResolveNamespace(Connection connection)
    {
        var keyName = CbsRequestProcessor.GetNamespaceForConnection(connection);
        if (!string.IsNullOrWhiteSpace(keyName))
            return keyName;

        try
        {
            var openProp = connection.GetType().GetProperty("Open",
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            if (openProp?.GetValue(connection) is Open open && !string.IsNullOrEmpty(open.HostName))
            {
                var host = open.HostName;
                var namespaceName = host.Split('.', 2)[0];
                if (!string.IsNullOrWhiteSpace(namespaceName)
                    && !namespaceName.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                    return namespaceName;
            }
        }
        catch
        {
            // Reflection failed; fall through to the default namespace.
        }

        return "default";
    }

    private static AttachContext? CreateAttachContext(ListenerLink link, Attach attach)
    {
        try
        {
            return (AttachContext?)AttachContextCtor?.Invoke([link, attach]);
        }
        catch (Exception ex)
        {
            Log.LogError(ex, "Reflection error creating AttachContext.");
            return null;
        }
    }

    private static RequestContext? CreateRequestContext(ListenerLink requestLink, ListenerLink responseLink, Message message)
    {
        try
        {
            return (RequestContext?)RequestContextCtor?.Invoke([requestLink, responseLink, message]);
        }
        catch (Exception ex)
        {
            Log.LogError(ex, "Reflection error creating RequestContext.");
            return null;
        }
    }

    /// <summary>
    /// Tracks the state for a registered request processor, including its
    /// request and response links (replicating ContainerHost's inner RequestProcessor class).
    /// </summary>
    internal class RequestProcessorEntry
    {
        public IRequestProcessor Processor { get; }
        public List<ListenerLink> RequestLinks { get; } = new();
        public Dictionary<string, ListenerLink> ResponseLinks { get; } = new(StringComparer.OrdinalIgnoreCase);

        public RequestProcessorEntry(IRequestProcessor processor)
        {
            Processor = processor;
        }
    }

}
