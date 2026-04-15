using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Server-side endpoint for receiving messages from clients.
/// When a client has a sender link, the server has a receiver endpoint.
/// </summary>
public class SenderLinkEndpoint : LinkEndpoint
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<SenderLinkEndpoint>();
    private readonly NamespaceContext _context;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;
    private readonly string _address;
    // AMQPNetLite dispatches OnMessage on a thread-pool thread per Transfer frame, so
    // multiple Transfer frames on the same link can be processed concurrently. Without
    // serialization, NextSequenceNumber and Enqueue race: thread B's later frame can
    // get a lower sequence number than thread A's earlier one, breaking FIFO ordering
    // (this manifests as session messages being delivered out of send order). Real ASB
    // and the official emulator preserve send order per link, so we hold a per-link lock
    // around routing to enforce the same.
    private readonly Lock _routeLock = new();

    public SenderLinkEndpoint(NamespaceContext context, string address, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _context = context;
        _address = address;
        _scheduledProcessor = scheduledProcessor;
    }

    public override void OnMessage(MessageContext messageContext)
    {
        try
        {
            var rawMsg = messageContext.Message;

            // Hold the per-link lock for the entire routing path so concurrent OnMessage
            // dispatches don't reorder NextSequenceNumber relative to Enqueue.
            lock (_routeLock)
            {
                // Detect Azure SDK batch messages: when the Azure SDK sends messages via
                // ServiceBusMessageBatch, it wraps them in a single AMQP transfer where the
                // body contains Data[] sections, each being a complete AMQP-encoded message.
                // The wrapper has minimal Properties (just MessageId) with no Subject.
                if (rawMsg.Body is Data[] dataArray && dataArray.Length > 0
                    && rawMsg.Properties?.Subject is null)
                {
                    Log.LogDebug("RECV BATCH ({Count} messages) → '{Address}'", dataArray.Length, _address);
                    foreach (var data in dataArray)
                    {
                        var innerMsg = Message.Decode(new ByteBuffer(data.Binary, 0, data.Binary.Length, data.Binary.Length));
                        var brokered = ConvertToBrokeredMessage(innerMsg);
                        Log.LogDebug("RECV {MessageId} → '{Address}' (batch)", brokered.MessageId, _address);
                        RouteMessage(_address, brokered);
                    }
                }
                else
                {
                    var brokered = ConvertToBrokeredMessage(rawMsg);
                    Log.LogDebug("RECV {MessageId} → '{Address}'", brokered.MessageId, _address);
                    RouteMessage(_address, brokered);
                }
            }

            messageContext.Complete();
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "RECV ERROR on '{Address}'", _address);

            messageContext.Complete(new global::Amqp.Framing.Error(new Symbol("amqp:internal-error"))
            {
                Description = "Failed to process message"
            });
        }
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // No-op: sender link endpoints do not need to handle flow.
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        dispositionContext.Complete();
    }

    /// <summary>
    /// Converts an AMQP message to a <see cref="BrokeredMessage"/>.
    /// Exposed as public for testing.
    /// </summary>
    public static BrokeredMessage ConvertToBrokeredMessage(Message amqpMessage)
    {
        var brokered = new BrokeredMessage();

        // Extract body
        if (amqpMessage.Body is byte[] bodyBytes)
        {
            brokered.Body = bodyBytes;
        }
        else if (amqpMessage.Body is Data data)
        {
            brokered.Body = data.Binary;
        }
        else if (amqpMessage.Body is Data[] dataArray)
        {
            // Multiple AMQP Data sections — concatenate into a single byte array.
            // Microsoft.Azure.Amqp can send messages this way when the body is large
            // or when the message is part of a batch.
            var totalLen = 0;
            foreach (var d in dataArray) totalLen += d.Binary.Length;
            var combined = new byte[totalLen];
            var offset = 0;
            foreach (var d in dataArray)
            {
                Buffer.BlockCopy(d.Binary, 0, combined, offset, d.Binary.Length);
                offset += d.Binary.Length;
            }
            brokered.Body = combined;
        }
        else if (amqpMessage.Body is AmqpValue amqpValue)
        {
            // AmqpValue body — serialize the value. The Azure SDK sometimes uses this
            // encoding for string or simple value messages.
            if (amqpValue.Value is byte[] valueBytes)
                brokered.Body = valueBytes;
            else if (amqpValue.Value is string valueStr)
                brokered.Body = System.Text.Encoding.UTF8.GetBytes(valueStr);
            else if (amqpValue.Value is not null)
                brokered.Body = System.Text.Encoding.UTF8.GetBytes(amqpValue.Value.ToString()!);
        }

        // Extract standard properties
        if (amqpMessage.Properties is not null)
        {
            var props = amqpMessage.Properties;

            if (props.MessageId is not null)
                brokered.MessageId = props.MessageId.ToString()!;
            if (props.CorrelationId is not null)
                brokered.CorrelationId = props.CorrelationId.ToString();
            if (props.ContentType is not null)
                brokered.ContentType = props.ContentType;
            if (props.Subject is not null)
                brokered.Subject = props.Subject;
            if (props.ReplyTo is not null)
                brokered.ReplyTo = props.ReplyTo;
            if (props.To is not null)
                brokered.To = props.To;
            if (props.GroupId is not null)
                brokered.SessionId = props.GroupId;
            if (props.ReplyToGroupId is not null)
                brokered.ReplyToSessionId = props.ReplyToGroupId;
        }

        // Extract application properties
        if (amqpMessage.ApplicationProperties?.Map is not null)
        {
            foreach (var kvp in amqpMessage.ApplicationProperties.Map)
            {
                brokered.ApplicationProperties[kvp.Key.ToString()!] = kvp.Value;
            }
        }

        // Extract message annotations
        if (amqpMessage.MessageAnnotations?.Map is not null)
        {
            var annotations = amqpMessage.MessageAnnotations.Map;

            if (annotations.TryGetValue(new Symbol("x-opt-scheduled-enqueue-time"), out var scheduledTime))
            {
                brokered.ScheduledEnqueueTimeUtc = scheduledTime switch
                {
                    DateTimeOffset dto => dto,
                    DateTime dt => new DateTimeOffset(dt, TimeSpan.Zero),
                    _ => null
                };
            }

            if (annotations.TryGetValue(new Symbol("x-opt-partition-key"), out var partitionKey))
            {
                brokered.PartitionKey = partitionKey?.ToString();
            }
        }

        // Extract TTL from header
        if (amqpMessage.Header?.Ttl > 0)
        {
            brokered.TimeToLive = TimeSpan.FromMilliseconds(amqpMessage.Header.Ttl);
        }

        return brokered;
    }

    /// <summary>
    /// Routes a brokered message to the appropriate queue or topic.
    /// Exposed as public for testing.
    /// </summary>
    internal void RouteMessage(string address, BrokeredMessage message)
    {
        message.SequenceNumber = _context.NextSequenceNumber();
        message.EnqueuedTimeUtc = DateTimeOffset.UtcNow;

        // Check if the message should be scheduled
        if (message.ScheduledEnqueueTimeUtc.HasValue
            && message.ScheduledEnqueueTimeUtc.Value > DateTimeOffset.UtcNow
            && _scheduledProcessor is not null)
        {
            _scheduledProcessor.Schedule(address, message, _context);
            return;
        }

        var (queue, topic) = _context.ResolveSendTarget(address);

        if (queue is not null)
        {
            queue.Enqueue(message);
        }
        else if (topic is not null)
        {
            topic.Publish(message);
        }
        else
        {
            throw new InvalidOperationException($"No queue or topic found for address '{address}'.");
        }
    }
}
