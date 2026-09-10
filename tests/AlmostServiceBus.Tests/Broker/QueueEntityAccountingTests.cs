using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

/// <summary>
/// MessageCount / TotalMessageCount / ConsumedCount bookkeeping and the bounded settled-message
/// history. These are what the dashboard shows, and under load they used to drift: a session
/// queue's MessageCount only ever went up, and every settled message was kept forever.
/// </summary>
public class QueueEntityAccountingTests
{
    private static BrokeredMessage CreateMessage(string? sessionId = null) => new()
    {
        Body = System.Text.Encoding.UTF8.GetBytes("hello"),
        SessionId = sessionId,
    };

    [Fact]
    public void SessionQueue_DequeueFromSession_DecrementsMessageCount_AndTracksPending()
    {
        var queue = new QueueEntity("orders") { RequiresSession = true };
        queue.Enqueue(CreateMessage("A"));
        queue.Enqueue(CreateMessage("A"));
        queue.Enqueue(CreateMessage("B"));
        Assert.Equal(3, queue.MessageCount);

        var sessionA = queue.Sessions!.TryAcceptSession("A", "receiver-1")!;
        Assert.True(queue.TryDequeueFromSession(sessionA, out var first));
        Assert.Equal(2, queue.MessageCount);
        Assert.Equal(1, first.DeliveryCount);
        Assert.NotEqual(default, first.LockedUntil);
        Assert.True(queue.IsLockValid(first.LockToken!));

        Assert.True(queue.TryDequeueFromSession(sessionA, out _));
        Assert.False(queue.TryDequeueFromSession(sessionA, out _));
        Assert.Equal(1, queue.MessageCount);

        // Completing does not touch MessageCount (the message already left the queue on dequeue).
        queue.Complete(first.LockToken!);
        Assert.Equal(1, queue.MessageCount);
        Assert.Equal(1, queue.ConsumedCount);
    }

    [Fact]
    public void SessionQueue_AbandonAfterDequeue_ReturnsToCount()
    {
        var queue = new QueueEntity("orders") { RequiresSession = true };
        queue.Enqueue(CreateMessage("A"));
        var session = queue.Sessions!.TryAcceptSession("A", "receiver-1")!;
        Assert.True(queue.TryDequeueFromSession(session, out var message));
        Assert.Equal(0, queue.MessageCount);

        queue.Abandon(message.LockToken!);

        Assert.Equal(1, queue.MessageCount);
    }

    [Fact]
    public async Task Complete_MovesMessageOutOfLiveSet_IntoBoundedHistory()
    {
        var queue = new QueueEntity("busy");
        const int total = 1000;
        for (var i = 0; i < total; i++)
        {
            queue.Enqueue(CreateMessage());
            var m = await queue.DequeueAsync();
            queue.Complete(m.LockToken!);
        }

        Assert.Equal(total, queue.TotalMessageCount);
        Assert.Equal(total, queue.ConsumedCount);
        Assert.Equal(0, queue.MessageCount);

        // Nothing active is left, and the history handed to the dashboard is capped.
        var peeked = queue.PeekMessages(int.MaxValue);
        Assert.All(peeked, m => Assert.Equal(MessageState.Consumed, m.State));
        Assert.InRange(peeked.Count, 1, 200);
        Assert.Empty(queue.PeekFromSequence(0, int.MaxValue));
    }

    [Fact]
    public async Task PeekMessages_ShowsActiveFirst_ThenMostRecentlySettled()
    {
        var queue = new QueueEntity("mixed");
        queue.Enqueue(CreateMessage());
        var settled = await queue.DequeueAsync();
        queue.Complete(settled.LockToken!);
        queue.Enqueue(CreateMessage());

        var peeked = queue.PeekMessages(10);

        Assert.Equal(2, peeked.Count);
        Assert.Equal(MessageState.Active, peeked[0].State);
        Assert.Equal(MessageState.Consumed, peeked[1].State);
        Assert.Equal(settled.SequenceNumber, peeked[1].SequenceNumber);
    }

    [Fact]
    public async Task DeadLetter_CountsAsSettled_NotConsumed()
    {
        var queue = new QueueEntity("dl");
        queue.Enqueue(CreateMessage());
        var m = await queue.DequeueAsync();

        queue.DeadLetter(m.LockToken!, "reason", "description");

        Assert.Equal(0, queue.ConsumedCount);
        Assert.Equal(1, queue.TotalMessageCount);
        Assert.Equal(1, queue.DeadLetterQueue.MessageCount);
        Assert.Equal(1, queue.DeadLetterQueue.TotalMessageCount);
        var history = queue.PeekMessages(10);
        Assert.Single(history);
        Assert.Equal(MessageState.DeadLettered, history[0].State);
    }

    [Fact]
    public async Task Redelivery_DoesNotInflateTotalMessageCount()
    {
        var queue = new QueueEntity("retry");
        queue.Enqueue(CreateMessage());
        var m = await queue.DequeueAsync();
        queue.Abandon(m.LockToken!);

        Assert.Equal(1, queue.TotalMessageCount);
        Assert.Equal(1, queue.MessageCount);
    }
}
