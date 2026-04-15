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

public record MessageEvent(
    MessageEventType Type,
    string Namespace,
    string Entity,
    string MessageId,
    long SequenceNumber,
    string? ContentType,
    string? BodyPreview,
    Dictionary<string, object>? ScalarProperties,
    DateTimeOffset Timestamp);

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
