using System.Collections.Concurrent;
using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;
using Microsoft.Extensions.Logging;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Handles $management node requests such as cancel-scheduled-message.
/// </summary>
public class ManagementLinkEndpoint : IRequestProcessor
{
    public int Credit => 100;

    private static readonly ILogger Log = AmqpLog.CreateLogger<ManagementLinkEndpoint>();

    private readonly NamespaceContext _context;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;
    private readonly QueueEntity? _scopedQueue;
    private readonly ConcurrentDictionary<string, string>? _senderLinkNames;

    public ManagementLinkEndpoint(NamespaceContext context, ScheduledMessageProcessor? scheduledProcessor = null, QueueEntity? scopedQueue = null, ConcurrentDictionary<string, string>? senderLinkNames = null)
    {
        _context = context;
        _scheduledProcessor = scheduledProcessor;
        _scopedQueue = scopedQueue;
        _senderLinkNames = senderLinkNames;
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
        catch (Exception)
        {
            // Swallow exceptions — the request context may already be completed/disposed
        }
    }

    private void HandleScheduleMessage(RequestContext requestContext)
    {
        var sequenceNumbers = new List<long>();

        if (_scheduledProcessor is not null && requestContext.Message.Body is Map scheduleBody)
        {
            var entityName = requestContext.Message.ApplicationProperties?["associated-link-name"]?.ToString();

            if (scheduleBody.TryGetValue(new Symbol("messages"), out var messagesObj) && messagesObj is List messagesList)
            {
                foreach (var item in messagesList)
                {
                    if (item is not Map msgMap) continue;

                    // Extract the inner AMQP message
                    Message? innerMessage = null;
                    if (msgMap.TryGetValue(new Symbol("message"), out var msgBytes) && msgBytes is byte[] rawMessage)
                    {
                        innerMessage = Message.Decode(new ByteBuffer(rawMessage, 0, rawMessage.Length, rawMessage.Length));
                    }

                    // Extract the message-id
                    string? messageId = null;
                    if (msgMap.TryGetValue(new Symbol("message-id"), out var mid))
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
                            var (resolvedQueue, resolvedTopic) = _context.ResolveSendTarget(candidateAddress);
                            if (resolvedQueue is not null || resolvedTopic is not null)
                                address = candidateAddress;
                        }

                        // If not resolved as an entity path, try the sender link name registry.
                        if (address is null
                            && !string.IsNullOrEmpty(entityName)
                            && _senderLinkNames?.TryGetValue(entityName, out var registeredPath) == true)
                        {
                            Log.LogDebug("schedule-message: resolved link name '{LinkName}' → entity '{Entity}'", entityName, registeredPath);
                            address = registeredPath;
                        }

                        address ??= _scopedQueue?.Name;

                        if (string.IsNullOrEmpty(address))
                        {
                            Log.LogWarning("schedule-message: no entity address could be resolved (associated-link-name='{LinkName}', scopedQueue=null). Message dropped.", entityName);
                            continue;
                        }

                        var seqNo = _scheduledProcessor.Schedule(address, brokered);
                        sequenceNumbers.Add(seqNo);
                    }
                }
            }
        }

        // Return the sequence numbers as the response
        var responseBody = new Map
        {
            { "sequence-numbers", sequenceNumbers.ToArray() }
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
            if (body.TryGetValue(new Symbol("sequence-numbers"), out var seqNumbers) && seqNumbers is long[] numbers)
            {
                foreach (var seqNo in numbers)
                {
                    _scheduledProcessor.CancelScheduled(seqNo);
                }
            }
        }

        ReplyOk(requestContext);
    }

    private void HandleRenewSessionLock(RequestContext requestContext)
    {
        // The Azure SDK sends session-id in the message body map, not in ApplicationProperties
        string? sessionId = null;
        if (requestContext.Message.Body is Map renewBody)
        {
            if (TryGetMapValue(renewBody, "session-id", out var sidObj))
                sessionId = sidObj?.ToString();
        }
        // Fallback: also check ApplicationProperties for compatibility
        sessionId ??= requestContext.Message.ApplicationProperties?["session-id"] as string;

        var sessionManager = FindSessionManager();
        if (sessionId is null || sessionManager is null)
        {
            SendErrorResponse(requestContext, 400, "Session ID required");
            return;
        }

        var lockedUntil = sessionManager.RenewSessionLock(sessionId);
        if (lockedUntil is null)
        {
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
        // The Azure SDK sends session-id in the message body map, not in ApplicationProperties
        string? sessionId = null;
        if (requestContext.Message.Body is Map getBody)
        {
            if (TryGetMapValue(getBody, "session-id", out var sidObj))
                sessionId = sidObj?.ToString();
        }
        // Fallback: also check ApplicationProperties for compatibility
        sessionId ??= requestContext.Message.ApplicationProperties?["session-id"] as string;

        if (sessionId is null)
        {
            SendErrorResponse(requestContext, 400, "Session ID required");
            return;
        }

        var sessionManager = FindSessionManager();
        var state = sessionManager?.GetSessionState(sessionId);

        var responseBody = new Map
        {
            { "session-state", state ?? Array.Empty<byte>() }
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
        // The Azure SDK sends session-id in the message body map, not in ApplicationProperties
        string? sessionId = null;
        byte[]? state = null;

        if (requestContext.Message.Body is Map setBody)
        {
            if (TryGetMapValue(setBody, "session-id", out var sidObj))
                sessionId = sidObj?.ToString();

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

        FindSessionManager()?.SetSessionState(sessionId, state);

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
