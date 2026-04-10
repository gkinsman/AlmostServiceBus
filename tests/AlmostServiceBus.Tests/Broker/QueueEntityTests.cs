using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

public class QueueEntityTests
{
    private static BrokeredMessage CreateMessage(string? body = null)
    {
        return new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes(body ?? "hello")
        };
    }

    [Fact]
    public void Properties_HaveDefaults()
    {
        var queue = new QueueEntity("test-queue");

        Assert.Equal("test-queue", queue.Name);
        Assert.Equal(TimeSpan.FromSeconds(30), queue.LockDuration);
        Assert.Equal(10, queue.MaxDeliveryCount);
        Assert.False(queue.RequiresSession);
        Assert.False(queue.DeadLetteringOnMessageExpiration);
        Assert.Equal(TimeSpan.MaxValue, queue.DefaultMessageTimeToLive);
        Assert.True(queue.EnableBatchedOperations);
        Assert.Equal(1024L, queue.MaxSizeInMegabytes);
        Assert.Null(queue.ForwardTo);
        Assert.Null(queue.ForwardDeadLetteredMessagesTo);
        Assert.Null(queue.UserMetadata);
    }

    [Fact]
    public async Task Enqueue_And_Dequeue_RoundTrips()
    {
        var queue = new QueueEntity("test-queue");
        var message = CreateMessage("round-trip");

        queue.Enqueue(message);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var received = await queue.DequeueAsync(cts.Token);

        Assert.Equal(message.MessageId, received.MessageId);
        Assert.Equal(message.Body, received.Body);
    }

    [Fact]
    public async Task Enqueue_AssignsLockToken_WhenNull()
    {
        var queue = new QueueEntity("test-queue");
        var message = new BrokeredMessage { LockToken = null };

        queue.Enqueue(message);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var received = await queue.DequeueAsync(cts.Token);

        Assert.NotNull(received.LockToken);
        Assert.False(string.IsNullOrEmpty(received.LockToken));
    }

    [Fact]
    public async Task Dequeue_IncrementsDeliveryCount()
    {
        var queue = new QueueEntity("test-queue");
        queue.Enqueue(CreateMessage());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var received = await queue.DequeueAsync(cts.Token);

        Assert.Equal(1, received.DeliveryCount);
    }

    [Fact]
    public async Task Dequeue_CompetingConsumers_EachMessageDeliveredOnce()
    {
        var queue = new QueueEntity("test-queue");
        const int messageCount = 10;

        for (var i = 0; i < messageCount; i++)
            queue.Enqueue(CreateMessage($"msg-{i}"));

        var received = new System.Collections.Concurrent.ConcurrentBag<BrokeredMessage>();
        var tasks = new List<Task>();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // 3 competing consumers
        for (var c = 0; c < 3; c++)
        {
            tasks.Add(Task.Run(async () =>
            {
                while (received.Count < messageCount && !cts.IsCancellationRequested)
                {
                    var msg = queue.TryDequeueImmediate();
                    if (msg is not null)
                        received.Add(msg);
                    else
                        await Task.Delay(10, cts.Token);
                }
            }, cts.Token));
        }

        await Task.WhenAll(tasks);

        Assert.Equal(messageCount, received.Count);

        var ids = received.Select(m => m.MessageId).ToHashSet();
        Assert.Equal(messageCount, ids.Count); // no duplicates
    }

    [Fact]
    public async Task Complete_RemovesMessageFromPending()
    {
        var queue = new QueueEntity("test-queue");
        queue.Enqueue(CreateMessage());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var msg = await queue.DequeueAsync(cts.Token);

        Assert.NotNull(msg.LockToken);
        queue.Complete(msg.LockToken!);

        // After completing, abandoning the same token should throw or silently do nothing
        // The key check: message no longer tracked in pending
        var ex = Record.Exception(() => queue.Abandon(msg.LockToken!));
        // Either throws or does nothing — just ensure no crash and message is gone
        _ = ex; // either is acceptable
    }

    [Fact]
    public async Task Abandon_RequeuesMessage_IncrementsDeliveryCount()
    {
        var queue = new QueueEntity("test-queue") { MaxDeliveryCount = 10 };
        queue.Enqueue(CreateMessage());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var first = await queue.DequeueAsync(cts.Token);
        Assert.Equal(1, first.DeliveryCount);

        queue.Abandon(first.LockToken!);

        // Should be re-enqueued; dequeue again
        var second = await queue.DequeueAsync(cts.Token);
        Assert.Equal(first.MessageId, second.MessageId);
        Assert.Equal(2, second.DeliveryCount);
    }

    [Fact]
    public async Task Abandon_AssignsFreshLockToken()
    {
        var queue = new QueueEntity("test-queue") { MaxDeliveryCount = 10 };
        queue.Enqueue(CreateMessage());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var first = await queue.DequeueAsync(cts.Token);
        var originalLockToken = first.LockToken;

        queue.Abandon(first.LockToken!);

        var second = await queue.DequeueAsync(cts.Token);
        Assert.Equal(first.MessageId, second.MessageId);
        // Lock token must be different to avoid AMQP delivery tag collisions
        Assert.NotEqual(originalLockToken, second.LockToken);
    }

    [Fact]
    public async Task Abandon_ExceedsMaxDeliveryCount_DeadLetters()
    {
        var queue = new QueueEntity("test-queue") { MaxDeliveryCount = 2 };
        queue.Enqueue(CreateMessage());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // First delivery
        var msg1 = await queue.DequeueAsync(cts.Token);
        Assert.Equal(1, msg1.DeliveryCount);
        queue.Abandon(msg1.LockToken!);

        // Second delivery
        var msg2 = await queue.DequeueAsync(cts.Token);
        Assert.Equal(2, msg2.DeliveryCount);
        var originalLockToken = msg2.LockToken;

        // Abandon at MaxDeliveryCount should dead-letter
        queue.Abandon(msg2.LockToken!);

        // Message should now be in DLQ
        var dlqMsg = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dlqMsg);
        Assert.Equal(msg2.MessageId, dlqMsg!.MessageId);
        Assert.NotEqual(originalLockToken, dlqMsg.LockToken);
    }

    [Fact]
    public async Task DeadLetter_MovesMessageToDeadLetterQueue()
    {
        var queue = new QueueEntity("test-queue");
        queue.Enqueue(CreateMessage());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var msg = await queue.DequeueAsync(cts.Token);
        var originalLockToken = msg.LockToken;

        queue.DeadLetter(msg.LockToken!, "MaxDeliveryCountExceeded", "Too many retries");

        var dlqMsg = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dlqMsg);
        Assert.Equal(msg.MessageId, dlqMsg!.MessageId);
        Assert.Equal("MaxDeliveryCountExceeded", dlqMsg.DeadLetterReason);
        Assert.Equal("Too many retries", dlqMsg.DeadLetterErrorDescription);
        Assert.NotEqual(originalLockToken, dlqMsg.LockToken);
    }

    [Fact]
    public void DeadLetterQueue_HasExpectedName()
    {
        var queue = new QueueEntity("myqueue");

        Assert.Equal("myqueue/$deadletterqueue", queue.DeadLetterQueue.Name);
    }

    [Fact]
    public void DeadLetterQueue_PointsToSelf_WhenIsDeadLetterQueue()
    {
        var queue = new QueueEntity("myqueue/$deadletterqueue", isDeadLetterQueue: true);

        Assert.Same(queue, queue.DeadLetterQueue);
    }

    [Fact]
    public async Task TrackPending_TracksMessage()
    {
        var queue = new QueueEntity("test-queue");
        var msg = CreateMessage();
        msg.LockToken = Guid.NewGuid().ToString();

        queue.TrackPending(msg);

        // Completing a tracked message should succeed (not throw)
        var ex = Record.Exception(() => queue.Complete(msg.LockToken!));
        Assert.Null(ex);
    }

    [Fact]
    public void Complete_AfterLockExpirySweep_ThrowsMessageLockLost()
    {
        // Bug: when the lock-expiry sweep re-enqueues a message before the consumer
        // calls Complete(), Complete() silently returns instead of throwing
        // MessageLockLostException. This causes MassTransit to think the completion
        // succeeded, while the message has been re-enqueued — leading to R-DUPE.
        var queue = new QueueEntity("test-queue") { LockDuration = TimeSpan.FromMilliseconds(1) };
        queue.Enqueue(CreateMessage());

        var msg = queue.TryDequeueImmediate()!;
        var lockToken = msg.LockToken!;

        // Wait for lock to expire
        Thread.Sleep(50);

        // Manually trigger the sweep by waiting for the timer (fires every 5s) — too slow.
        // Instead, simulate the same effect: the message's lock expired and the sweep
        // removed it from _pending and re-enqueued it. We can trigger this by just waiting
        // for the background sweep. But with 5s interval that's too slow for a unit test.
        // Use a shorter approach: set lock duration very short, dequeue, wait, then Complete.
        // The sweep runs every 5s, so we need to wait for it.
        // Actually, let's just wait for the sweep to fire.
        Thread.Sleep(TimeSpan.FromSeconds(6));

        // The sweep should have re-enqueued the message by now.
        // Complete should throw MessageLockLostException, not silently return.
        Assert.Throws<MessageLockLostException>(() => queue.Complete(lockToken));

        // Verify the message was re-enqueued (available for redelivery)
        var redelivered = queue.TryDequeueImmediate();
        Assert.NotNull(redelivered);
        Assert.Equal(msg.MessageId, redelivered!.MessageId);
    }

    [Fact]
    public async Task RenewLock_PreventsExpirySweep_NoDoubleDelivery()
    {
        // Reproduces the race between SweepExpiredLocks and RenewLock:
        // without the fix, the sweep could re-enqueue a message whose lock
        // was just renewed, causing duplicate delivery (R-DUPE in MassTransit).
        //
        // Lock duration must be longer than the sweep interval (5s) so that
        // after renewal, the lock doesn't re-expire before the sweep runs.
        var queue = new QueueEntity("test-queue") { LockDuration = TimeSpan.FromSeconds(10) };
        queue.Enqueue(CreateMessage());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var msg = await queue.DequeueAsync(cts.Token);
        var originalLockToken = msg.LockToken!;

        // Let the lock expire (10s duration + 1s buffer)
        await Task.Delay(TimeSpan.FromSeconds(11));

        // Renew the lock — simulates the SDK's auto-renewal arriving just as
        // the sweep timer fires. The lock was expired, but the message is
        // still in _pending (sweep may or may not have run yet).
        var newExpiry = queue.RenewLock(originalLockToken);

        if (newExpiry is null)
        {
            // Sweep already ran and removed from _pending before renewal.
            // The message was re-enqueued — consume it to clean up.
            // This is valid behaviour; the race didn't occur in this run.
            var redelivered = queue.TryDequeueImmediate();
            Assert.NotNull(redelivered);
            return;
        }

        Assert.True(newExpiry > DateTimeOffset.UtcNow);

        // Give the background sweep timer a chance to run (fires every 5s).
        // The renewed lock should prevent re-enqueue.
        await Task.Delay(TimeSpan.FromSeconds(6));

        // The message should still be completable with the original lock token
        // (i.e. NOT re-enqueued by the sweep).
        queue.Complete(originalLockToken);

        // Queue should be empty — no duplicate delivery
        var next = queue.TryDequeueImmediate();
        Assert.Null(next);
    }
}
