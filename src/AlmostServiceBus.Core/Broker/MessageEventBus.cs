using System.Threading.Channels;

namespace AlmostServiceBus.Core.Broker;

public enum MessageEventType
{
    Enqueued,
    Completed,
    DeadLettered,
    Abandoned,
    Deferred,
    NamespaceCreated
}

/// <summary>
/// A broker-side message lifecycle notification streamed to the dashboard. Enqueued events
/// carry enough of the message (body preview, application properties, subject, correlation
/// id) for the dashboard to render a row without a follow-up fetch; the settlement events
/// identify the message only.
/// </summary>
public record MessageEvent(
    MessageEventType Type,
    string Namespace,
    string Entity,
    string MessageId,
    long SequenceNumber,
    string? ContentType,
    string? BodyPreview,
    Dictionary<string, object>? ScalarProperties,
    DateTimeOffset Timestamp,
    Dictionary<string, object>? ApplicationProperties = null,
    string? Subject = null,
    string? CorrelationId = null);

public class MessageEventBus
{
    private readonly List<Channel<MessageEvent>> _subscribers = [];
    private readonly Lock _lock = new();

    public ChannelReader<MessageEvent> Subscribe()
    {
        var channel = Channel.CreateBounded<MessageEvent>(
            new BoundedChannelOptions(256) { FullMode = BoundedChannelFullMode.DropOldest });
        lock (_lock)
        {
            _subscribers.Add(channel);
        }
        return channel.Reader;
    }

    public void Unsubscribe(ChannelReader<MessageEvent> reader)
    {
        lock (_lock)
        {
            _subscribers.RemoveAll(c => c.Reader == reader);
        }
    }

    public void Publish(MessageEvent evt)
    {
        lock (_lock)
        {
            foreach (var channel in _subscribers)
            {
                channel.Writer.TryWrite(evt);
            }
        }
    }
}
