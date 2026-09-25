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

    // ── Admin operations (dashboard) ─────────────────────────────────────────

    [Fact]
    public void ListScheduled_IsScopedToNamespace_AndOrderedByTime()
    {
        var ns = new NamespaceContext("tenant-a");
        var other = new NamespaceContext("tenant-b");
        var processor = new ScheduledMessageProcessor(ns);
        var now = DateTimeOffset.UtcNow;

        var later = processor.Schedule("q1", CreateMessage(now.AddHours(2)), ns);
        var sooner = processor.Schedule("q2", CreateMessage(now.AddHours(1)), ns);
        processor.Schedule("q1", CreateMessage(now.AddMinutes(1)), other);

        var listed = processor.ListScheduled("tenant-a");
        Assert.Equal(new[] { sooner, later }, listed.Select(m => m.Message.SequenceNumber));

        var q1Only = processor.ListScheduled("TENANT-A", "q1");
        Assert.Equal("q1", Assert.Single(q1Only).EntityName);
    }

    [Fact]
    public void Shift_MovesOnlyTheNamespaceAndEntityAsked()
    {
        var ns = new NamespaceContext("tenant-a");
        var other = new NamespaceContext("tenant-b");
        var processor = new ScheduledMessageProcessor(ns);
        var t = DateTimeOffset.UtcNow.AddHours(1);

        var a1 = processor.Schedule("q1", CreateMessage(t), ns);
        var a2 = processor.Schedule("q2", CreateMessage(t), ns);
        var b1 = processor.Schedule("q1", CreateMessage(t), other);

        Assert.Equal(1, processor.Shift("tenant-a", TimeSpan.FromMinutes(-30), "q1"));
        Assert.Equal(t.AddMinutes(-30), processor.GetScheduledBySequence("tenant-a", a1)!.ScheduledEnqueueTimeUtc);
        Assert.Equal(t, processor.GetScheduledBySequence("tenant-a", a2)!.ScheduledEnqueueTimeUtc);

        Assert.Equal(2, processor.Shift("tenant-a", TimeSpan.FromMinutes(10)));
        Assert.Equal(t.AddMinutes(-20), processor.GetScheduledBySequence("tenant-a", a1)!.ScheduledEnqueueTimeUtc);
        Assert.Equal(t.AddMinutes(10), processor.GetScheduledBySequence("tenant-a", a2)!.ScheduledEnqueueTimeUtc);
        Assert.Equal(t, processor.GetScheduledBySequence("tenant-b", b1)!.ScheduledEnqueueTimeUtc);
    }

    [Fact]
    public void DeliverAllNow_LeavesOtherNamespacesScheduled()
    {
        var ns = new NamespaceContext("tenant-a");
        var other = new NamespaceContext("tenant-b");
        var queueA = ns.CreateQueue("q");
        var queueB = other.CreateQueue("q");
        var processor = new ScheduledMessageProcessor(ns);
        var future = DateTimeOffset.UtcNow.AddDays(1);

        processor.Schedule("q", CreateMessage(future), ns);
        processor.Schedule("q", CreateMessage(future), ns);
        processor.Schedule("q", CreateMessage(future), other);

        Assert.Equal(2, processor.DeliverAllNow("tenant-a"));

        Assert.NotNull(queueA.TryDequeueImmediate());
        Assert.NotNull(queueA.TryDequeueImmediate());
        Assert.Null(queueB.TryDequeueImmediate());
        Assert.Single(processor.ListScheduled("tenant-b"));
    }

    [Fact]
    public void Shift_IntoThePast_DeliversOnNextPoll()
    {
        var ns = CreateNamespace();
        var queue = ns.CreateQueue("my-queue");
        var processor = new ScheduledMessageProcessor(ns);
        processor.Schedule("my-queue", CreateMessage(DateTimeOffset.UtcNow.AddHours(1)));

        Assert.Equal(1, processor.Shift(ns.Name, TimeSpan.FromHours(-2)));
        processor.ProcessDueMessages();

        var delivered = queue.TryDequeueImmediate();
        Assert.NotNull(delivered);
        Assert.Null(delivered!.ScheduledEnqueueTimeUtc);
        Assert.Empty(processor.ListScheduled(ns.Name));
    }

    [Fact]
    public void Shift_KeepsSequenceNumber_SoClientCancelStillWorks()
    {
        var ns = CreateNamespace();
        var processor = new ScheduledMessageProcessor(ns);
        var seqNo = processor.Schedule("my-queue", CreateMessage(DateTimeOffset.UtcNow.AddHours(1)));

        processor.Shift(ns.Name, TimeSpan.FromHours(3));

        Assert.NotNull(processor.GetScheduledBySequence(ns.Name, seqNo));
        Assert.True(processor.CancelScheduled(seqNo, ns));
    }

    [Fact]
    public void Shift_UnknownNamespace_MovesNothing()
    {
        var ns = new NamespaceContext("tenant-a");
        var processor = new ScheduledMessageProcessor(ns);
        var t = DateTimeOffset.UtcNow.AddHours(1);
        var seqNo = processor.Schedule("q", CreateMessage(t), ns);

        Assert.Equal(0, processor.Shift("tenant-b", TimeSpan.FromHours(-1)));
        Assert.Equal(t, processor.GetScheduledBySequence("tenant-a", seqNo)!.ScheduledEnqueueTimeUtc);
    }
}
