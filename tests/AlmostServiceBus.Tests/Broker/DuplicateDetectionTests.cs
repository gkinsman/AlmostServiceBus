using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Time.Testing;

namespace AlmostServiceBus.Tests.Broker;

// Pins the documented ASB behaviour (learn.microsoft.com/azure/service-bus-messaging/duplicate-detection).
// MassTransit's delayed redelivery reschedules with the original MessageId, so it is dropped on a
// duplicate-detection queue — on real ASB too. Don't exempt scheduled or forwarded messages.
public class DuplicateDetectionTests
{
    private static BrokeredMessage Message(string messageId, DateTimeOffset? scheduledTime = null, string? sessionId = null) => new()
    {
        MessageId = messageId,
        Body = System.Text.Encoding.UTF8.GetBytes("x"),
        ScheduledEnqueueTimeUtc = scheduledTime,
        SessionId = sessionId
    };

    private static TopicEntity DedupTopic(NamespaceContext ns, params string[] subscriptions)
    {
        var topic = ns.CreateTopic("topic");
        topic.RequiresDuplicateDetection = true;
        topic.DuplicateDetectionHistoryTimeWindow = TimeSpan.FromSeconds(30);
        foreach (var sub in subscriptions)
            ns.CreateSubscription("topic", sub);
        return topic;
    }

    private static QueueEntity DedupQueue(NamespaceContext ns, string name = "dedup")
    {
        var queue = ns.CreateQueue(name);
        queue.RequiresDuplicateDetection = true;
        queue.DuplicateDetectionHistoryTimeWindow = TimeSpan.FromSeconds(30);
        return queue;
    }

    [Fact]
    public void SendWithSeenMessageId_IsDropped()
    {
        var queue = DedupQueue(new NamespaceContext("ns"));

        queue.Enqueue(Message("a"));
        queue.Enqueue(Message("a"));
        queue.Enqueue(Message("b"));

        Assert.Equal(2, queue.MessageCount);
    }

    [Fact]
    public void ScheduledMessageWithSeenMessageId_IsDropped()
    {
        // "if you send a non-scheduled message and then a duplicate scheduled message,
        // the scheduled message is dropped."
        var ns = new NamespaceContext("ns");
        var queue = DedupQueue(ns);
        var processor = new ScheduledMessageProcessor(ns);

        queue.Enqueue(Message("a"));
        Assert.NotNull(queue.TryDequeueImmediate());

        processor.Schedule("dedup", Message("a", DateTimeOffset.UtcNow.AddHours(-1)));
        processor.ProcessDueMessages();

        Assert.Null(queue.TryDequeueImmediate());
    }

    [Fact]
    public void ForwardedMessage_IsRecordedByDestinationDuplicateDetection()
    {
        // Microsoft's advice for adding duplicate detection to an existing entity is to forward
        // it into a new duplicate-detection entity, so forwarded copies are checked at the destination.
        var ns = new NamespaceContext("ns");
        var queue = DedupQueue(ns);
        ns.CreateSubscription("topic", "fwd", forwardTo: "dedup");

        ns.GetTopic("topic")!.Publish(Message("a"));
        ns.GetTopic("topic")!.Publish(Message("a"));
        queue.Enqueue(Message("a"));

        Assert.Equal(1, queue.MessageCount);
    }

    [Fact]
    public void MessagesWithoutMessageId_AreNeverDuplicates()
    {
        var queue = DedupQueue(new NamespaceContext("ns"));

        queue.Enqueue(Message(""));
        queue.Enqueue(Message(""));

        Assert.Equal(2, queue.MessageCount);
    }

    [Fact]
    public void Queue_SeenMessageIdIsDroppedUntilWindowEnds()
    {
        var time = new FakeTimeProvider();
        var queue = DedupQueue(new NamespaceContext("ns", timeProvider: time));

        queue.Enqueue(Message("a"));
        time.Advance(TimeSpan.FromSeconds(29));
        queue.Enqueue(Message("a"));
        time.Advance(TimeSpan.FromSeconds(2));
        queue.Enqueue(Message("a"));

        Assert.Equal(2, queue.MessageCount);
    }

    [Fact]
    public void SessionQueue_DropsSeenMessageIdAcrossSessions()
    {
        var queue = DedupQueue(new NamespaceContext("ns"));
        queue.RequiresSession = true;

        queue.Enqueue(Message("a", sessionId: "s1"));
        queue.Enqueue(Message("a", sessionId: "s1"));
        queue.Enqueue(Message("a", sessionId: "s2"));

        Assert.Equal(1, queue.MessageCount);
    }

    [Fact]
    public void SessionQueue_SendRejectedForMissingSessionId_DoesNotBlockRetry()
    {
        var queue = DedupQueue(new NamespaceContext("ns"));
        queue.RequiresSession = true;

        Assert.Throws<SessionRequiredException>(() => queue.Enqueue(Message("a")));
        queue.Enqueue(Message("a", sessionId: "s1"));

        Assert.Equal(1, queue.MessageCount);
    }

    [Fact]
    public void Topic_DropsSeenMessageIdForEverySubscription()
    {
        var ns = new NamespaceContext("ns");
        var topic = DedupTopic(ns, "s1", "s2");

        topic.Publish(Message("a"));
        topic.Publish(Message("a"));
        topic.Publish(Message("b"));

        Assert.Equal(2, topic.GetSubscription("s1")!.Queue.MessageCount);
        Assert.Equal(2, topic.GetSubscription("s2")!.Queue.MessageCount);
    }

    [Fact]
    public void Topic_SeenMessageIdIsDroppedUntilWindowEnds()
    {
        var time = new FakeTimeProvider();
        var topic = DedupTopic(new NamespaceContext("ns", timeProvider: time), "s1");

        topic.Publish(Message("a"));
        time.Advance(TimeSpan.FromSeconds(29));
        topic.Publish(Message("a"));
        time.Advance(TimeSpan.FromSeconds(2));
        topic.Publish(Message("a"));

        Assert.Equal(2, topic.GetSubscription("s1")!.Queue.MessageCount);
    }

    [Fact]
    public void Topic_ForwardedMessageWithSeenMessageId_IsDropped()
    {
        var ns = new NamespaceContext("ns");
        var topic = DedupTopic(ns, "s1");
        ns.CreateQueue("source").ForwardTo = "topic";

        ns.GetQueue("source")!.Enqueue(Message("a"));
        topic.Publish(Message("a"));

        Assert.Equal(1, topic.GetSubscription("s1")!.Queue.MessageCount);
    }
}
