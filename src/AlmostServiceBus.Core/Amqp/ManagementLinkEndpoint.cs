using System.Collections.Concurrent;
using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Handles $management node requests such as cancel-scheduled-message.
/// </summary>
public class ManagementLinkEndpoint : IRequestProcessor
{
    public int Credit => 100;

    private static readonly ILogger Log = AmqpLog.CreateLogger<ManagementLinkEndpoint>();

    private readonly NamespaceContext _context;
    private readonly NamespaceRegistry? _registry;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;
    private readonly string? _scopedAddress;
    private readonly QueueEntity? _scopedQueue;
    private readonly ConcurrentDictionary<string, EmulatorContainer.SenderLinkTarget>? _senderLinkNames;

    public ManagementLinkEndpoint(NamespaceContext context, ScheduledMessageProcessor? scheduledProcessor = null, string? scopedAddress = null, QueueEntity? scopedQueue = null, ConcurrentDictionary<string, EmulatorContainer.SenderLinkTarget>? senderLinkNames = null, NamespaceRegistry? registry = null)
    {
        _context = context;
        _registry = registry;
        _scheduledProcessor = scheduledProcessor;
        _scopedAddress = scopedAddress;
        _scopedQueue = scopedQueue;
        _senderLinkNames = senderLinkNames;
    }

    /// <summary>
    /// Resolves the namespace context for the given request by looking up the AMQP connection's
    /// CBS-authenticated namespace. Falls back to <see cref="_context"/> when the registry is
    /// unavailable or no per-connection namespace has been registered.
    /// </summary>
    private NamespaceContext ResolveNamespace(RequestContext requestContext)
    {
        if (_registry is null)
            return _context;

        var connection = requestContext.Link.Session.Connection;
        var keyName = CbsRequestProcessor.GetNamespaceForConnection(connection);
        return keyName is not null ? _registry.GetOrCreate(keyName) : _context;
    }

    public void Process(RequestContext requestContext)
    {
        var operation = requestContext.Message.ApplicationProperties?["operation"]?.ToString();
        try
        {
            switch (operation)
            {
                case "com.microsoft:cancel-scheduled-message":
                    HandleCancelScheduledMessage(requestContext);
                    break;

                case "com.microsoft:schedule-message":
                    HandleScheduleMessage(requestContext);
                    break;

                case "com.microsoft:renew-lock":
                    HandleRenewLock(requestContext);
                    break;

                case "com.microsoft:renew-session-lock":
                    HandleRenewSessionLock(requestContext);
                    break;

                case "com.microsoft:get-session-state":
                    HandleGetSessionState(requestContext);
                    break;

                case "com.microsoft:set-session-state":
                    HandleSetSessionState(requestContext);
                    break;

                default:
                    ReplyOk(requestContext);
                    break;
            }
        }
        catch (Exception ex)
        {
            Log.LogError(ex, "MGMT exception handling operation={Operation}", operation);
        }
    }

    private void HandleScheduleMessage(RequestContext requestContext)
    {
        var sequenceNumbers = new List<long>();

        if (_scheduledProcessor is not null && requestContext.Message.Body is Map scheduleBody)
        {
            // Resolve the namespace for this request from the connection's CBS authentication.
            // The global $management endpoint uses a shared defaultContext, but each
            // per-namespace client has its entities in its own namespace. Using the connection's
            // namespace ensures entities are found and scheduled messages are delivered correctly.
            var scheduleContext = ResolveNamespace(requestContext);

            var entityName = requestContext.Message.ApplicationProperties?["associated-link-name"]?.ToString();

            if (TryGetMapValue(scheduleBody, "messages", out var messagesObj) && messagesObj is List messagesList)
            {
                foreach (var item in messagesList)
                {
                    if (item is not Map msgMap) continue;

                    // Extract the inner AMQP message
                    Message? innerMessage = null;
                    if (TryGetMapValue(msgMap, "message", out var msgBytes) && msgBytes is byte[] rawMessage)
                    {
                        innerMessage = Message.Decode(new ByteBuffer(rawMessage, 0, rawMessage.Length, rawMessage.Length));
                    }

                    // Extract the message-id
                    string? messageId = null;
                    if (TryGetMapValue(msgMap, "message-id", out var mid))
                        messageId = mid?.ToString();

                    if (innerMessage is not null)
                    {
                        var brokered = SenderLinkEndpoint.ConvertToBrokeredMessage(innerMessage);
                        if (messageId is not null)
                            brokered.MessageId = messageId;

                        // Resolve the entity to schedule on.
                        // The associated-link-name is typically a GUID sender link name (e.g.,
                        // "sender-abc123"), NOT the entity path. Attempt to resolve it as an
                        // entity path first; if that fails, look up the link name in the sender
                        // link registry, then fall back to the scoped queue on entity-level
                        // management links.
                        var candidateAddress = entityName?.TrimStart('/');
                        string? address = null;
                        if (!string.IsNullOrEmpty(candidateAddress))
                        {
                            var (resolvedQueue, resolvedTopic) = scheduleContext.ResolveSendTarget(candidateAddress);
                            if (resolvedQueue is not null || resolvedTopic is not null)
                                address = candidateAddress;
                        }

                        // If not resolved as an entity path, try the sender link name registry.
                        if (address is null
                            && !string.IsNullOrEmpty(entityName)
                            && _senderLinkNames is not null)
                        {
                            foreach (var senderLinkKey in EmulatorContainer.BuildSenderLinkRegistryKeys(requestContext.Link.Session.Connection, entityName))
                            {
                                if (_senderLinkNames.TryGetValue(senderLinkKey, out var registeredTarget))
                                {
                                    Log.LogDebug("schedule-message: resolved scoped link name '{LinkName}' → entity '{Entity}' in namespace '{Namespace}'", entityName, registeredTarget.Address, registeredTarget.NamespaceName);
                                    address = registeredTarget.Address;
                                    scheduleContext = _registry?.GetOrCreate(registeredTarget.NamespaceName) ?? scheduleContext;
                                    break;
                                }
                            }
                        }

                        if (address is null && !string.IsNullOrEmpty(brokered.To))
                        {
                            var explicitAddress = brokered.To.TrimStart('/');
                            var (resolvedQueue, resolvedTopic) = scheduleContext.ResolveSendTarget(explicitAddress);
                            if (resolvedQueue is not null || resolvedTopic is not null)
                                address = explicitAddress;
                        }

                        address ??= _scopedAddress ?? _scopedQueue?.Name;

                        if (string.IsNullOrEmpty(address))
                        {
                            Log.LogWarning("schedule-message: no entity address could be resolved (associated-link-name='{LinkName}', scopedQueue=null). Message dropped.", entityName);
                            continue;
                        }

                        Log.LogWarning("schedule-message: address={Address}, SessionId={SessionId}, MessageId={MessageId}, Subject={Subject}",
                            address, brokered.SessionId, brokered.MessageId, brokered.Subject);
                        var seqNo = _scheduledProcessor.Schedule(address, brokered, scheduleContext);
                        sequenceNumbers.Add(seqNo);
                    }
                }
            }
        }

        // Return the sequence numbers as the response.
        // AMQPNetLite's WriteArray has a bug where encoding an empty long[] produces
        // malformed bytes (size=1 with no data). The Azure SDK's parser then throws
        // amqp:decode-error. Use Amqp.Types.List when empty; non-empty long[] is fine.
        object seqNumbersValue = sequenceNumbers.Count > 0
            ? (object)sequenceNumbers.ToArray()
            : new List();
        var responseBody = new Map
        {
            { "sequence-numbers", seqNumbersValue }
        };
        var response = new Message(responseBody)
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["statusCode"] = 200,
                ["statusDescription"] = "OK"
            },
            Properties = new Properties
            {
                CorrelationId = requestContext.Message.Properties?.MessageId
            }
        };
        requestContext.Complete(response);
    }

    private void HandleRenewLock(RequestContext requestContext)
    {
        var expirations = new List<DateTime>();

        if (requestContext.Message.Body is Map renewBody
            && TryGetMapValue(renewBody, "lock-tokens", out var tokensObj)
            && tokensObj is Guid[] lockTokenGuids)
        {
            // Try to find the queue. The entity name may be in the associated-link-name
            // or we scan all queues for the lock token.
            foreach (var lockGuid in lockTokenGuids)
            {
                var lockToken = lockGuid.ToString();
                DateTimeOffset? newExpiry = null;

                // Scan all queues in the namespace for the lock token
                foreach (var queue in _context.GetQueues())
                {
                    newExpiry = queue.RenewLock(lockToken);
                    if (newExpiry.HasValue) break;

                    // Also check the queue's DLQ
                    newExpiry = queue.DeadLetterQueue.RenewLock(lockToken);
                    if (newExpiry.HasValue) break;
                }

                // Also check subscription queues
                if (!newExpiry.HasValue)
                {
                    foreach (var topic in _context.GetTopics())
                    {
                        foreach (var sub in topic.GetSubscriptions())
                        {
                            newExpiry = sub.Queue.RenewLock(lockToken);
                            if (newExpiry.HasValue) break;
                        }
                        if (newExpiry.HasValue) break;
                    }
                }

                expirations.Add(newExpiry?.UtcDateTime ?? DateTime.UtcNow.AddMinutes(5));
            }
        }

        var responseBody = new Map
        {
            { "expirations", expirations.ToArray() }
        };
        var response = new Message(responseBody)
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["statusCode"] = 200,
                ["statusDescription"] = "OK"
            },
            Properties = new Properties
            {
                CorrelationId = requestContext.Message.Properties?.MessageId
            }
        };
        requestContext.Complete(response);
    }

    private void HandleCancelScheduledMessage(RequestContext requestContext)
    {
        if (_scheduledProcessor is not null && requestContext.Message.Body is Map body)
        {
            var scheduleContext = ResolveNamespace(requestContext);
            if (TryGetMapValue(body, "sequence-numbers", out var seqNumbers) && seqNumbers is long[] numbers)
            {
                foreach (var seqNo in numbers)
                {
                    _scheduledProcessor.CancelScheduled(seqNo, scheduleContext);
                }
            }
        }

        ReplyOk(requestContext);
    }

    private void HandleRenewSessionLock(RequestContext requestContext)
    {
        var sessionId = ExtractSessionId(requestContext);

        var sessionManager = FindSessionManager();
        Log.LogWarning(
            "HandleRenewSessionLock: sessionId={SessionId}, scopedAddress={ScopedAddress}, scopedQueue={ScopedQueue}, sessionManagerFound={Found}",
            sessionId, _scopedAddress, _scopedQueue?.Name, sessionManager is not null);

        if (sessionId is null || sessionManager is null)
        {
            SendErrorResponse(requestContext, 400, "Session ID required");
            return;
        }

        var lockedUntil = sessionManager.RenewSessionLock(sessionId);
        if (lockedUntil is null)
        {
            Log.LogWarning(
                "HandleRenewSessionLock: session '{SessionId}' not found or not locked. Known sessions: [{Sessions}]",
                sessionId, string.Join(", ", sessionManager.GetSessionIds()));
            SendErrorResponse(requestContext, 404, "Session not found or not locked");
            return;
        }

        var responseBody = new Map
        {
            { "expiration", lockedUntil.Value.UtcDateTime }
        };
        var response = new Message(responseBody)
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["statusCode"] = 200,
                ["statusDescription"] = "OK"
            },
            Properties = new Properties { CorrelationId = requestContext.Message.Properties?.MessageId }
        };
        requestContext.Complete(response);
    }

    private void HandleGetSessionState(RequestContext requestContext)
    {
        var sessionId = ExtractSessionId(requestContext);

        if (sessionId is null)
        {
            SendErrorResponse(requestContext, 400, "Session ID required");
            return;
        }

        var sessionManager = FindSessionManager();
        if (sessionManager is null)
        {
            SendErrorResponse(requestContext, 400, "Entity does not support sessions");
            return;
        }

        var state = sessionManager.GetSessionState(sessionId);
        Log.LogWarning("HandleGetSessionState: sessionId={SessionId}, scopedAddress={ScopedAddress}, stateLength={Length}",
            sessionId, _scopedAddress, state?.Length);
        var responseBody = new Map
        {
            { "session-state", state ?? (object)null! }
        };
        var response = new Message(responseBody)
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["statusCode"] = 200,
                ["statusDescription"] = "OK"
            },
            Properties = new Properties { CorrelationId = requestContext.Message.Properties?.MessageId }
        };
        requestContext.Complete(response);
    }

    private void HandleSetSessionState(RequestContext requestContext)
    {
        var sessionId = ExtractSessionId(requestContext);
        byte[]? state = null;

        if (requestContext.Message.Body is Map setBody)
        {
            if (TryGetMapValue(setBody, "session-state", out var stateObj))
            {
                state = stateObj switch
                {
                    byte[] bytes => bytes.Length > 0 ? bytes : null,
                    null => null,
                    _ => null
                };
            }
        }
        // Fallback: also check ApplicationProperties for compatibility
        sessionId ??= requestContext.Message.ApplicationProperties?["session-id"] as string;

        if (sessionId is null)
        {
            SendErrorResponse(requestContext, 400, "Session ID required");
            return;
        }

        var sessionManager = FindSessionManager();
        if (sessionManager is null)
        {
            SendErrorResponse(requestContext, 400, "Entity does not support sessions");
            return;
        }

        Log.LogWarning("HandleSetSessionState: sessionId={SessionId}, scopedAddress={ScopedAddress}, stateLength={Length}, scopedQueueSessionManager={SameManager}",
            sessionId, _scopedAddress, state?.Length, ReferenceEquals(sessionManager, _scopedQueue?.Sessions));
        sessionManager.SetSessionState(sessionId, state);

        var response = new Message()
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["statusCode"] = 200,
                ["statusDescription"] = "OK"
            },
            Properties = new Properties { CorrelationId = requestContext.Message.Properties?.MessageId }
        };
        requestContext.Complete(response);
    }

    /// <summary>
    /// Finds the SessionManager for session operations. Uses the scoped queue if available,
    /// Extracts a session ID from a management request message body or application properties.
    /// </summary>
    private static string? ExtractSessionId(RequestContext requestContext)
    {
        string? sessionId = null;
        if (requestContext.Message.Body is Map body)
        {
            if (TryGetMapValue(body, "session-id", out var sidObj))
                sessionId = sidObj?.ToString();
        }
        sessionId ??= requestContext.Message.ApplicationProperties?["session-id"] as string;
        return sessionId;
    }

    /// <summary>
    /// otherwise searches all queues in the namespace.
    /// </summary>
    private SessionManager? FindSessionManager()
    {
        if (_scopedQueue?.Sessions is not null)
            return _scopedQueue.Sessions;

        // Fallback: search all queues in the context for one with sessions
        foreach (var queue in _context.GetQueues())
        {
            if (queue.Sessions is not null)
                return queue.Sessions;
        }
        return null;
    }

    private void SendErrorResponse(RequestContext requestContext, int statusCode, string description)
    {
        var response = new Message()
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["statusCode"] = statusCode,
                ["statusDescription"] = description
            },
            Properties = new Properties { CorrelationId = requestContext.Message.Properties?.MessageId }
        };
        requestContext.Complete(response);
    }

    /// <summary>
    /// Looks up a key in an AMQP Map, trying both Symbol and String key types.
    /// The Azure SDK's Microsoft.Azure.Amqp library encodes map keys as strings,
    /// while AMQPNetLite uses Symbols.
    /// </summary>
    private static bool TryGetMapValue(Map map, string key, out object? value)
    {
        if (map.TryGetValue(new Symbol(key), out value))
            return true;
        if (map.TryGetValue(key, out value))
            return true;
        value = null;
        return false;
    }

    private static void ReplyOk(RequestContext requestContext)
    {
        var response = new Message()
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["statusCode"] = 200,
                ["statusDescription"] = "OK"
            },
            Properties = new Properties
            {
                CorrelationId = requestContext.Message.Properties?.MessageId
            }
        };
        requestContext.Complete(response);
    }
}
