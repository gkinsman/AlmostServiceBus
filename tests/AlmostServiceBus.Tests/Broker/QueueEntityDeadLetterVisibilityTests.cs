using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

/// <summary>
/// A dead-lettered message must be visible where it now lives (the DLQ) and gone from where it
/// came from, regardless of which path put it there. Explicit dead-letters used to stamp
/// <see cref="MessageState.DeadLettered"/> on the shared object, so the DLQ's peek (dashboard and
/// SDK) filtered them out; abandon-past-max-delivery left them in the source queue's live view.
/// </summary>
public class QueueEntityDeadLetterVisibilityTests
{
    private static BrokeredMessage CreateMessage() => new()
    {
        Body = System.Text.Encoding.UTF8.GetBytes("{\"orderId\":\"1\"}"),
        ApplicationProperties = { ["source"] = "test" },
    };

    [Fact]
    public async Task ExplicitDeadLetter_IsPeekableInDlq_AndGoneFromSource()
    {
        var queue = new QueueEntity("orders");
        queue.Enqueue(CreateMessage());
        var m = await queue.DequeueAsync();

        queue.DeadLetter(m.LockToken!, "ValidationFailed", "bad schema");

        var dlq = queue.DeadLetterQueue;
        var inDlq = Assert.Single(dlq.PeekFromSequence(0, 10));
        Assert.Equal(MessageState.Active, inDlq.State);
        Assert.Equal("ValidationFailed", inDlq.DeadLetterReason);
        Assert.Equal("bad schema", inDlq.DeadLetterErrorDescription);
        Assert.Equal("orders", inDlq.DeadLetterSource);
        Assert.Equal("test", inDlq.ApplicationProperties["source"]);

        var dashboardDlq = dlq.PeekMessages(10);
        Assert.Single(dashboardDlq);
        Assert.Equal(MessageState.Active, dashboardDlq[0].State);

        Assert.Empty(queue.PeekFromSequence(0, 10));
        var history = Assert.Single(queue.PeekMessages(10));
        Assert.Equal(MessageState.DeadLettered, history.State);
        Assert.Equal("ValidationFailed", history.DeadLetterReason);
        Assert.Equal(1, dlq.MessageCount);
        Assert.Equal(0, queue.MessageCount);
    }

    [Fact]
    public async Task AbandonPastMaxDeliveryCount_LeavesSourceLiveView()
    {
        var queue = new QueueEntity("orders") { MaxDeliveryCount = 2 };
        queue.Enqueue(CreateMessage());

        var first = await queue.DequeueAsync();
        queue.Abandon(first.LockToken!);
        await Task.Delay(TimeSpan.FromSeconds(1.5)); // redelivery is delayed by 1s
        var second = await queue.DequeueAsync();
        queue.Abandon(second.LockToken!);

        Assert.Empty(queue.PeekFromSequence(0, 10));
        Assert.DoesNotContain(queue.PeekMessages(10), x => x.State == MessageState.Active);
        var history = Assert.Single(queue.PeekMessages(10));
        Assert.Equal(MessageState.DeadLettered, history.State);
        Assert.Equal("MaxDeliveryCountExceeded", history.DeadLetterReason);

        var inDlq = Assert.Single(queue.DeadLetterQueue.PeekFromSequence(0, 10));
        Assert.Equal(MessageState.Active, inDlq.State);
        Assert.Equal(2, inDlq.DeliveryCount);
    }

    [Fact]
    public async Task DlqMessage_CanBeReceivedAndCompleted()
    {
        var queue = new QueueEntity("orders");
        queue.Enqueue(CreateMessage());
        var m = await queue.DequeueAsync();
        queue.DeadLetter(m.LockToken!, "r", "d");

        var fromDlq = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(fromDlq);
        queue.DeadLetterQueue.Complete(fromDlq.LockToken!);

        Assert.Equal(0, queue.DeadLetterQueue.MessageCount);
        Assert.Equal(1, queue.DeadLetterQueue.ConsumedCount);
        Assert.Empty(queue.DeadLetterQueue.PeekFromSequence(0, 10));
    }

    [Fact]
    public void EnqueuedEvent_CarriesApplicationPropertiesSubjectAndCorrelationId()
    {
        var bus = new MessageEventBus();
        var reader = bus.Subscribe();
        var queue = new QueueEntity("orders");
        queue.SetEventBus(bus, "ns", "orders");

        queue.Enqueue(new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("x"),
            Subject = "OrderPlaced",
            CorrelationId = "corr-1",
            ApplicationProperties = { ["region"] = "uk", ["attempt"] = 3 },
        });

        Assert.True(reader.TryRead(out var evt));
        Assert.Equal(MessageEventType.Enqueued, evt.Type);
        Assert.Equal("OrderPlaced", evt.Subject);
        Assert.Equal("corr-1", evt.CorrelationId);
        Assert.NotNull(evt.ApplicationProperties);
        Assert.Equal("uk", evt.ApplicationProperties["region"]);
        Assert.Equal(3, evt.ApplicationProperties["attempt"]);
    }

    [Fact]
    public void EnqueuedEvent_OmitsApplicationProperties_WhenNone()
    {
        var bus = new MessageEventBus();
        var reader = bus.Subscribe();
        var queue = new QueueEntity("orders");
        queue.SetEventBus(bus, "ns", "orders");

        queue.Enqueue(new BrokeredMessage { Body = System.Text.Encoding.UTF8.GetBytes("x") });

        Assert.True(reader.TryRead(out var evt));
        Assert.Null(evt.ApplicationProperties);
    }
}
