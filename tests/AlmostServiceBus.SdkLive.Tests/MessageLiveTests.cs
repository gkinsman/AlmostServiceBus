// Ported from Azure.Messaging.ServiceBus.Tests.Message.MessageLiveTests
using Azure.Messaging.ServiceBus;

namespace AlmostServiceBus.SdkLive.Tests;

public class MessageLiveTests : SdkLiveTestBase
{
    [Fact]
    public async Task MessagePropertiesRoundTrip()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);

        var msg = new ServiceBusMessage("test body")
        {
            MessageId = "test-message-id",
            Subject = "test-subject",
            ContentType = "application/json",
            CorrelationId = "test-correlation",
            To = "test-to",
            ReplyTo = "test-reply-to",
            ReplyToSessionId = "test-reply-session",
            TimeToLive = TimeSpan.FromMinutes(5),
        };
        msg.ApplicationProperties["key1"] = "value1";
        msg.ApplicationProperties["key2"] = 42;
        msg.ApplicationProperties["key3"] = true;

        await sender.SendMessageAsync(msg);

        var receiver = Client.CreateReceiver(queueName);
        var received = await receiver.ReceiveMessageAsync();

        Assert.NotNull(received);
        Assert.Equal("test body", received.Body.ToString());
        Assert.Equal("test-message-id", received.MessageId);
        Assert.Equal("test-subject", received.Subject);
        Assert.Equal("application/json", received.ContentType);
        Assert.Equal("test-correlation", received.CorrelationId);
        Assert.Equal("test-to", received.To);
        Assert.Equal("test-reply-to", received.ReplyTo);
        Assert.Equal("test-reply-session", received.ReplyToSessionId);
        Assert.Equal("value1", received.ApplicationProperties["key1"]);
        Assert.Equal(42, received.ApplicationProperties["key2"]);
        Assert.Equal(true, received.ApplicationProperties["key3"]);
        Assert.True(received.SequenceNumber > 0);
        Assert.True(received.EnqueuedTime != default, "EnqueuedTime should be set");
        Assert.Equal(1, received.DeliveryCount);

        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task SendEmptyMessage()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(new ServiceBusMessage());

        var receiver = Client.CreateReceiver(queueName);
        var received = await receiver.ReceiveMessageAsync();
        Assert.NotNull(received);
        Assert.Empty(received.Body.ToArray());
        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task SendLargeMessage()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        var body = GetRandomBuffer(50_000);
        await sender.SendMessageAsync(new ServiceBusMessage(body));

        var receiver = Client.CreateReceiver(queueName);
        var received = await receiver.ReceiveMessageAsync();
        Assert.NotNull(received);
        Assert.Equal(body, received.Body.ToArray());
        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task SessionMessagePropertiesRoundTrip()
    {
        var queueName = await CreateQueueAsync(enableSession: true);
        var sender = Client.CreateSender(queueName);
        var sessionId = "test-session-123";

        var msg = new ServiceBusMessage("session test body")
        {
            SessionId = sessionId,
            MessageId = "session-msg-id",
            Subject = "session-subject",
        };
        msg.ApplicationProperties["session-key"] = "session-value";

        await sender.SendMessageAsync(msg);

        var receiver = await Client.AcceptSessionAsync(queueName, sessionId);
        var received = await receiver.ReceiveMessageAsync();

        Assert.NotNull(received);
        Assert.Equal("session test body", received.Body.ToString());
        Assert.Equal(sessionId, received.SessionId);
        Assert.Equal("session-msg-id", received.MessageId);
        Assert.Equal("session-subject", received.Subject);
        Assert.Equal("session-value", received.ApplicationProperties["session-key"]);

        await receiver.CompleteMessageAsync(received);
    }

    [Fact]
    public async Task TopicSubscriptionRoundTrip()
    {
        var (topicName, subs) = await CreateTopicAsync(subscriptions: ["sub-a", "sub-b"]);
        var sender = Client.CreateSender(topicName);

        var msg = new ServiceBusMessage("topic test body")
        {
            MessageId = "topic-msg-id",
            Subject = "topic-subject",
        };
        msg.ApplicationProperties["topic-key"] = "topic-value";
        await sender.SendMessageAsync(msg);

        // Both subscriptions should receive the message
        foreach (var sub in subs)
        {
            var receiver = Client.CreateReceiver(topicName, sub);
            var received = await receiver.ReceiveMessageAsync();
            Assert.NotNull(received);
            Assert.Equal("topic test body", received.Body.ToString());
            Assert.Equal("topic-msg-id", received.MessageId);
            Assert.Equal("topic-subject", received.Subject);
            Assert.Equal("topic-value", received.ApplicationProperties["topic-key"]);
            await receiver.CompleteMessageAsync(received);
        }
    }

    [Fact]
    public async Task SequenceNumberIncrementsAcrossMessages()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);

        for (int i = 0; i < 5; i++)
            await sender.SendMessageAsync(GetMessage());

        var receiver = Client.CreateReceiver(queueName);
        long lastSeq = 0;
        for (int i = 0; i < 5; i++)
        {
            var msg = await receiver.ReceiveMessageAsync();
            Assert.NotNull(msg);
            Assert.True(msg.SequenceNumber > lastSeq, $"SequenceNumber should increment: got {msg.SequenceNumber} after {lastSeq}");
            lastSeq = msg.SequenceNumber;
            await receiver.CompleteMessageAsync(msg);
        }
    }

    [Fact]
    public async Task DeliveryCountIncrementsOnAbandon()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        var receiver = Client.CreateReceiver(queueName);

        var msg = await receiver.ReceiveMessageAsync();
        Assert.NotNull(msg);
        Assert.Equal(1, msg.DeliveryCount);
        await receiver.AbandonMessageAsync(msg);

        msg = await receiver.ReceiveMessageAsync();
        Assert.NotNull(msg);
        Assert.Equal(2, msg.DeliveryCount);
        await receiver.CompleteMessageAsync(msg);
    }
}
