using Amqp.Framing;
using Amqp.Types;
using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Amqp;

public class ReceiverLinkEndpointTests
{
    private static QueueEntity CreateQueueWithMessage(string body = "hello", string? lockToken = null)
    {
        var queue = new QueueEntity("test-queue");
        var message = new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes(body),
            LockToken = lockToken ?? Guid.NewGuid().ToString(),
            SequenceNumber = 1,
            EnqueuedTimeUtc = DateTimeOffset.UtcNow
        };
        queue.Enqueue(message);
        return queue;
    }

    [Fact]
    public async Task DequeueAsync_DeliversMessages()
    {
        var queue = CreateQueueWithMessage("test message");
        var endpoint = new ReceiverLinkEndpoint(queue);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var message = await endpoint.DequeueAsync(cts.Token);

        Assert.NotNull(message);
        Assert.Equal("test message", System.Text.Encoding.UTF8.GetString(message.Body));
        Assert.Equal(1, message.DeliveryCount);
    }

    [Fact]
    public void CompleteMessage_RemovesFromPending()
    {
        var queue = new QueueEntity("test-queue");
        var lockToken = Guid.NewGuid().ToString();
        var message = new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello"),
            LockToken = lockToken,
        };
        queue.Enqueue(message);

        // Dequeue to track as pending
        var dequeued = queue.TryDequeueImmediate()!;
        Assert.NotNull(dequeued);

        var endpoint = new ReceiverLinkEndpoint(queue);
        endpoint.SettleMessage(lockToken, new Accepted());

        // Completing the same lock token again should be a no-op (already removed)
        // Verify by trying to abandon — should not re-enqueue since it's already gone
        queue.Abandon(lockToken);
        var reDequeued = queue.TryDequeueImmediate();
        Assert.Null(reDequeued);
    }

    [Fact]
    public async Task AbandonMessage_RequeuesMessage()
    {
        var queue = new QueueEntity("test-queue");
        var lockToken = Guid.NewGuid().ToString();
        var message = new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello"),
            LockToken = lockToken,
        };
        queue.Enqueue(message);

        // Dequeue to track as pending
        var dequeued = queue.TryDequeueImmediate()!;
        Assert.NotNull(dequeued);

        var endpoint = new ReceiverLinkEndpoint(queue);
        endpoint.SettleMessage(lockToken, new Released());

        // Message should be re-enqueued (after 1s redelivery delay)
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var reDequeued = await queue.DequeueAsync(cts.Token);
        Assert.NotNull(reDequeued);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(reDequeued.Body));
    }

    [Fact]
    public void DeadLetterMessage_MovesToDLQ()
    {
        var queue = new QueueEntity("test-queue");
        var lockToken = Guid.NewGuid().ToString();
        var message = new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello"),
            LockToken = lockToken,
        };
        queue.Enqueue(message);

        // Dequeue to track as pending
        var dequeued = queue.TryDequeueImmediate()!;
        Assert.NotNull(dequeued);

        var endpoint = new ReceiverLinkEndpoint(queue);
        endpoint.SettleMessage(lockToken, new Rejected
        {
            Error = new Error(new Symbol("amqp:rejected"))
            {
                Description = "Bad message"
            }
        });

        // Message should be in dead letter queue
        var dlqMessage = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dlqMessage);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(dlqMessage.Body));
    }

    [Fact]
    public void ModifiedUndeliverableHere_MovesToDLQ()
    {
        var queue = new QueueEntity("test-queue");
        var lockToken = Guid.NewGuid().ToString();
        var message = new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello"),
            LockToken = lockToken,
        };
        queue.Enqueue(message);

        var dequeued = queue.TryDequeueImmediate()!;
        Assert.NotNull(dequeued);

        var endpoint = new ReceiverLinkEndpoint(queue);
        endpoint.SettleMessage(lockToken, new Modified { UndeliverableHere = true });

        var dlqMessage = queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dlqMessage);
    }

    [Fact]
    public async Task ModifiedDeliverable_RequeuesMessage()
    {
        var queue = new QueueEntity("test-queue");
        var lockToken = Guid.NewGuid().ToString();
        var message = new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes("hello"),
            LockToken = lockToken,
        };
        queue.Enqueue(message);

        var dequeued = queue.TryDequeueImmediate()!;
        Assert.NotNull(dequeued);

        var endpoint = new ReceiverLinkEndpoint(queue);
        endpoint.SettleMessage(lockToken, new Modified { UndeliverableHere = false });

        // Message should be re-enqueued (after 1s redelivery delay)
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var reDequeued = await queue.DequeueAsync(cts.Token);
        Assert.NotNull(reDequeued);
    }

    [Fact]
    public void ConvertToAmqpMessage_SetsAllFields()
    {
        var brokered = new BrokeredMessage
        {
            MessageId = "msg-1",
            CorrelationId = "corr-1",
            ContentType = "text/plain",
            Subject = "subj",
            ReplyTo = "reply",
            To = "dest",
            SessionId = "sess-1",
            ReplyToSessionId = "reply-sess-1",
            Body = System.Text.Encoding.UTF8.GetBytes("payload"),
            SequenceNumber = 42,
            DeliveryCount = 3,
            LockToken = Guid.NewGuid().ToString(),
            EnqueuedTimeUtc = DateTimeOffset.UtcNow,
            ApplicationProperties = new Dictionary<string, object>
            {
                ["custom"] = "value"
            }
        };

        var amqp = ReceiverLinkEndpoint.ConvertToAmqpMessage(brokered);

        Assert.Equal("msg-1", amqp.Properties.MessageId.ToString());
        Assert.Equal("corr-1", amqp.Properties.CorrelationId.ToString());
        Assert.Equal("text/plain", (string)amqp.Properties.ContentType);
        Assert.Equal("subj", amqp.Properties.Subject);
        Assert.Equal("reply", amqp.Properties.ReplyTo);
        Assert.Equal("dest", amqp.Properties.To);
        Assert.Equal("sess-1", amqp.Properties.GroupId);
        Assert.Equal("reply-sess-1", amqp.Properties.ReplyToGroupId);
        // AMQP Header.DeliveryCount is 0-based (prior unsuccessful deliveries).
        // The broker's DeliveryCount=3 maps to AMQP DeliveryCount=2 (3-1).
        Assert.Equal(2u, amqp.Header.DeliveryCount);
        Assert.Equal("value", amqp.ApplicationProperties["custom"]);

        // Check message annotations
        Assert.Equal(42L, amqp.MessageAnnotations[new Symbol("x-opt-sequence-number")]);
        Assert.NotNull(amqp.MessageAnnotations[new Symbol("x-opt-lock-token")]);
    }
}
