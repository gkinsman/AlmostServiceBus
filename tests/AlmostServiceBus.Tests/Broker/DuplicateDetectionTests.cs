using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

// Pins the documented ASB behaviour (learn.microsoft.com/azure/service-bus-messaging/duplicate-detection).
// MassTransit's delayed redelivery reschedules with the original MessageId, so it is dropped on a
// duplicate-detection queue — on real ASB too. Don't exempt scheduled or forwarded messages.
public class DuplicateDetectionTests
{
    private static BrokeredMessage Message(string messageId, DateTimeOffset? scheduledTime = null) => new()
    {
        MessageId = messageId,
        Body = System.Text.Encoding.UTF8.GetBytes("x"),
        ScheduledEnqueueTimeUtc = scheduledTime
    };

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
}
