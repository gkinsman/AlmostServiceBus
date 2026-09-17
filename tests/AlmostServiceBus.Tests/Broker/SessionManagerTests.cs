using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

public class SessionManagerTests
{
    [Fact]
    public void Session_Delivers_Messages_In_SequenceNumber_Order()
    {
        var session = new SessionState("test-session");

        // Enqueue messages in reverse sequence-number order (simulating concurrent race)
        session.Enqueue(new BrokeredMessage { SequenceNumber = 3, Body = "Three"u8.ToArray() });
        session.Enqueue(new BrokeredMessage { SequenceNumber = 1, Body = "One"u8.ToArray() });
        session.Enqueue(new BrokeredMessage { SequenceNumber = 2, Body = "Two"u8.ToArray() });

        Assert.Equal(3, session.MessageCount);

        // Dequeue should return in sequence-number order regardless of insertion order
        Assert.True(session.TryDequeue(out var msg1));
        Assert.True(session.TryDequeue(out var msg2));
        Assert.True(session.TryDequeue(out var msg3));
        Assert.False(session.TryDequeue(out _));

        Assert.Equal(1, msg1!.SequenceNumber);
        Assert.Equal(2, msg2!.SequenceNumber);
        Assert.Equal(3, msg3!.SequenceNumber);
        Assert.Equal(0, session.MessageCount);
    }

    [Fact]
    public async Task Session_Delivers_Messages_In_SequenceNumber_Order_Under_Concurrent_Enqueue()
    {
        var session = new SessionState("test-session");
        const int messageCount = 50;

        // Use a barrier to maximize concurrent contention
        var barrier = new Barrier(messageCount);

        var tasks = Enumerable.Range(1, messageCount).Select(seq => Task.Run(() =>
        {
            var msg = new BrokeredMessage
            {
                SequenceNumber = seq,
                Body = System.Text.Encoding.UTF8.GetBytes($"msg-{seq}")
            };
            barrier.SignalAndWait();
            session.Enqueue(msg);
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(messageCount, session.MessageCount);

        // Dequeue all — should come out in sequence-number order
        var dequeued = new List<long>();
        while (session.TryDequeue(out var msg))
        {
            dequeued.Add(msg!.SequenceNumber);
        }

        Assert.Equal(messageCount, dequeued.Count);
        for (int i = 0; i < messageCount; i++)
        {
            Assert.Equal(i + 1, dequeued[i]);
        }
    }

    [Fact]
    public async Task Session_WaitToReadAsync_Signals_On_Enqueue()
    {
        var session = new SessionState("test-session");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Start a waiter
        var waitTask = session.WaitToReadAsync(cts.Token);

        // Enqueue a message
        session.Enqueue(new BrokeredMessage { SequenceNumber = 1 });

        // Wait should complete
        await waitTask;

        Assert.True(session.TryDequeue(out var msg));
        Assert.Equal(1, msg!.SequenceNumber);
    }

    [Fact]
    public void SessionManager_Enqueue_Routes_To_Correct_Session()
    {
        var mgr = new SessionManager(TimeSpan.FromSeconds(30));

        mgr.Enqueue(new BrokeredMessage { SessionId = "A", SequenceNumber = 1 });
        mgr.Enqueue(new BrokeredMessage { SessionId = "B", SequenceNumber = 2 });
        mgr.Enqueue(new BrokeredMessage { SessionId = "A", SequenceNumber = 3 });

        var sessionA = mgr.TryAcceptSession("A", "receiver-1");
        var sessionB = mgr.TryAcceptSession("B", "receiver-2");

        Assert.NotNull(sessionA);
        Assert.NotNull(sessionB);

        Assert.Equal(2, sessionA!.MessageCount);
        Assert.Equal(1, sessionB!.MessageCount);

        // Session A should deliver in sequence order
        Assert.True(sessionA.TryDequeue(out var a1));
        Assert.True(sessionA.TryDequeue(out var a2));
        Assert.Equal(1, a1!.SequenceNumber);
        Assert.Equal(3, a2!.SequenceNumber);
    }

    [Fact]
    public void SessionQueue_MessageCount_TracksWaitingMessagesNotEnqueueTotal()
    {
        // A session queue hands its messages to the SessionManager; the queue-level
        // counter used to only ever grow (it was never decremented on the session
        // dequeue path), so the dashboard showed more active messages than had ever
        // been enqueued once redeliveries kicked in.
        var queue = new QueueEntity("session-queue") { RequiresSession = true };
        queue.Enqueue(new BrokeredMessage { SessionId = "a", Body = "1"u8.ToArray() });
        queue.Enqueue(new BrokeredMessage { SessionId = "a", Body = "2"u8.ToArray() });
        queue.Enqueue(new BrokeredMessage { SessionId = "b", Body = "3"u8.ToArray() });
        Assert.Equal(3, queue.MessageCount);

        var session = queue.Sessions!.TryAcceptSession("a", "receiver-1");
        Assert.NotNull(session);
        Assert.True(session!.TryDequeue(out var first));
        queue.TrackPending(first!);
        Assert.Equal(2, queue.MessageCount);

        queue.Complete(first!.LockToken!);
        Assert.Equal(2, queue.MessageCount);

        Assert.True(session.TryDequeue(out var second));
        queue.TrackPending(second!);
        queue.Abandon(second!.LockToken!);
        // Abandon re-enqueues after a 1s delay; the message is in flight (not waiting)
        // during that window, then returns to the session.
        Assert.Equal(1, queue.MessageCount);
    }
}
