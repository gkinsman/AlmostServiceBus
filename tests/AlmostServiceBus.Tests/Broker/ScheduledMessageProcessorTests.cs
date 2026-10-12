using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

public class ScheduledMessageProcessorTests
{
    private static NamespaceContext CreateNamespace() => new("test-ns");

    private static BrokeredMessage CreateMessage(DateTimeOffset? scheduledTime = null)
    {
        return new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello"),
            ScheduledEnqueueTimeUtc = scheduledTime
        };
    }

    [Fact]
    public void Schedule_ReturnsSequenceNumber_GreaterThanZero()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);

        var seqNo = processor.Schedule("my-queue", CreateMessage());

        Assert.True(seqNo > 0);
    }

    [Fact]
    public void CancelScheduled_ReturnsTrueIfFound()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);

        var seqNo = processor.Schedule("my-queue", CreateMessage());
        var result = processor.CancelScheduled(seqNo);

        Assert.True(result);
    }

    [Fact]
    public void CancelScheduled_ReturnsFalseIfNotFound()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);

        var result = processor.CancelScheduled(99999L);

        Assert.False(result);
    }

    [Fact]
    public void ProcessDueMessages_DeliversWhenDue()
    {
        var ns = CreateNamespace();
        var queue = ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);

        // Schedule a message with a time in the past
        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        processor.Schedule("my-queue", CreateMessage(pastTime));

        processor.ProcessDueMessages();

        var delivered = queue.TryDequeueImmediate();
        Assert.NotNull(delivered);
    }

    [Fact]
    public void ProcessDueMessages_DoesNotDeliverFutureMessages()
    {
        var ns = CreateNamespace();
        var queue = ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);

        // Schedule a message 1 hour in the future
        var futureTime = DateTimeOffset.UtcNow.AddHours(1);
        processor.Schedule("my-queue", CreateMessage(futureTime));

        processor.ProcessDueMessages();

        var delivered = queue.TryDequeueImmediate();
        Assert.Null(delivered);
    }

    [Fact]
    public void ProcessDueMessages_ClearsScheduledEnqueueTimeUtc_WhenDelivered()
    {
        var ns = CreateNamespace();
        var queue = ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);

        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        processor.Schedule("my-queue", CreateMessage(pastTime));

        processor.ProcessDueMessages();

        var delivered = queue.TryDequeueImmediate();
        Assert.NotNull(delivered);
        Assert.Null(delivered!.ScheduledEnqueueTimeUtc);
    }

    [Fact]
    public void ScheduleToTopic_FansOutWhenDue()
    {
        var ns = CreateNamespace();
        var targetQueue = ns.CreateQueue("target-queue");

        // Create topic with a subscription that forwards to target-queue
        ns.CreateSubscription("my-topic", "sub1", forwardTo: "target-queue");

        var processor = new ScheduledMessageProcessor(ns);

        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        processor.Schedule("my-topic", CreateMessage(pastTime));

        processor.ProcessDueMessages();

        var delivered = targetQueue.TryDequeueImmediate();
        Assert.NotNull(delivered);
    }

    [Fact]
    public void ProcessDueMessages_DoesNotCollideAcrossNamespacesWithSameSequenceNumber()
    {
        var defaultNs = new NamespaceContext("default");
        var otherNs = new NamespaceContext("other");
        var defaultQueue = defaultNs.CreateQueue("queue-a");
        var otherQueue = otherNs.CreateQueue("queue-b");
        var processor = new ScheduledMessageProcessor(defaultNs);

        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        var seq1 = processor.Schedule("queue-a", CreateMessage(pastTime), defaultNs);
        var seq2 = processor.Schedule("queue-b", CreateMessage(pastTime), otherNs);

        Assert.Equal(1L, seq1);
        Assert.Equal(1L, seq2);

        processor.ProcessDueMessages();

        Assert.NotNull(defaultQueue.TryDequeueImmediate());
        Assert.NotNull(otherQueue.TryDequeueImmediate());
    }

    [Fact]
    public void Dispose_DoesNotThrow()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);
        processor.StartBackground(TimeSpan.FromSeconds(1));

        // Dispose should not throw
        processor.Dispose();
    }

    [Fact]
    public void CancelScheduled_AfterProcess_ReturnsFalse()
    {
        var ns = CreateNamespace();
        ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);

        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        var seqNo = processor.Schedule("my-queue", CreateMessage(pastTime));

        processor.ProcessDueMessages();

        // Message was already delivered, so cancellation should return false
        var result = processor.CancelScheduled(seqNo);
        Assert.False(result);
    }
    // The queue only becomes session-required after scheduling, which is how a message that
    // would now be refused at schedule time can still be sitting in the store when it falls due.
    private static (QueueEntity Good, ScheduledMessageProcessor Processor) ArrangeUndeliverableAndDeliverable()
    {
        var ns = CreateNamespace();
        var bad = ns.CreateQueue("session-queue");
        var good = ns.CreateQueue("plain-queue");
        var processor = new ScheduledMessageProcessor(ns);

        var pastTime = DateTimeOffset.UtcNow.AddHours(-1);
        processor.Schedule("session-queue", CreateMessage(pastTime));
        bad.RequiresSession = true;
        processor.Schedule("plain-queue", CreateMessage(pastTime));

        return (good, processor);
    }

    [Fact]
    public void ProcessDueMessages_UndeliverableMessage_DoesNotStopOtherDeliveries()
    {
        var (good, processor) = ArrangeUndeliverableAndDeliverable();

        processor.ProcessDueMessages();

        Assert.NotNull(good.TryDequeueImmediate());
        Assert.Empty(processor.GetScheduledForEntity("test-ns", "session-queue"));
    }

    [Fact]
    public async Task Background_UndeliverableMessage_LaterScheduledMessagesStillDelivered()
    {
        var ns = CreateNamespace();
        var bad = ns.CreateQueue("session-queue");
        var good = ns.CreateQueue("plain-queue");
        using var processor = new ScheduledMessageProcessor(ns);

        processor.Schedule("session-queue", CreateMessage(DateTimeOffset.UtcNow.AddHours(-1)));
        bad.RequiresSession = true;
        processor.StartBackground(TimeSpan.FromMilliseconds(10));

        await WaitUntilAsync(() => !processor.GetScheduledForEntity("test-ns", "session-queue").Any());

        processor.Schedule("plain-queue", CreateMessage(DateTimeOffset.UtcNow.AddMilliseconds(50)));

        BrokeredMessage? delivered = null;
        await WaitUntilAsync(() => (delivered = good.TryDequeueImmediate()) is not null);
        Assert.NotNull(delivered);
    }

    [Fact]
    public void ProcessDueMessages_TopicWithSessionSubscription_NonSessionMessage_DoesNotThrow()
    {
        var ns = CreateNamespace();
        ns.CreateSubscription("my-topic", "plain");
        var sessionSub = ns.CreateSubscription("my-topic", "sessions");
        sessionSub.Queue.RequiresSession = true;
        var processor = new ScheduledMessageProcessor(ns);

        processor.Schedule("my-topic", CreateMessage(DateTimeOffset.UtcNow.AddHours(-1)));

        processor.ProcessDueMessages();
    }

    [Fact]
    public void ProcessDueMessages_TargetDeleted_DropsMessage()
    {
        var ns = CreateNamespace();
        ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);
        processor.Schedule("my-queue", CreateMessage(DateTimeOffset.UtcNow.AddHours(-1)));
        ns.DeleteQueue("my-queue");

        processor.ProcessDueMessages();

        Assert.Empty(processor.GetScheduledForEntity("test-ns", "my-queue"));
    }

    [Fact]
    public void Schedule_NonSessionMessageToSessionQueue_Throws()
    {
        var ns = CreateNamespace();
        ns.CreateQueue("session-queue").RequiresSession = true;
        var processor = new ScheduledMessageProcessor(ns);

        Assert.Throws<SessionRequiredException>(() =>
            processor.Schedule("session-queue", CreateMessage(DateTimeOffset.UtcNow.AddHours(1))));
        Assert.Empty(processor.GetScheduledForEntity("test-ns", "session-queue"));
    }

    [Fact]
    public void Schedule_SessionMessageToSessionQueue_Succeeds()
    {
        var ns = CreateNamespace();
        ns.CreateQueue("session-queue").RequiresSession = true;
        var processor = new ScheduledMessageProcessor(ns);
        var message = CreateMessage(DateTimeOffset.UtcNow.AddHours(1));
        message.SessionId = "s1";

        processor.Schedule("session-queue", message);

        Assert.Single(processor.GetScheduledForEntity("test-ns", "session-queue"));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition not met within 5 seconds.");
            await Task.Delay(10);
        }
    }
}
