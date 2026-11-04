using System.Net;
using System.Net.Sockets;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

public class MessageExpiryTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private ServiceBusAdministrationClient _admin = null!;
    private ServiceBusClient _client = null!;

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();

        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (context, ct) =>
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(IPAddress.Loopback, context.DnsEndPoint.Port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
        };
        _admin = new ServiceBusAdministrationClient(
            _fixture.ConnectionString,
            new ServiceBusAdministrationClientOptions { Transport = new HttpClientTransport(new HttpClient(handler)) });

        _client = new ServiceBusClient(_fixture.ConnectionString, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
        });
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _fixture.DisposeAsync();
    }

    [Fact]
    public async Task ReceivedMessage_TtlCappedAtQueueDefault_PeekShowsSentTtl()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("ttl-cap") { DefaultMessageTimeToLive = TimeSpan.FromSeconds(30) });
        await _client.CreateSender("ttl-cap").SendMessageAsync(new ServiceBusMessage("x") { TimeToLive = TimeSpan.FromHours(1) });
        var receiver = _client.CreateReceiver("ttl-cap");

        var peeked = await receiver.PeekMessageAsync();
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(TimeSpan.FromHours(1), peeked!.TimeToLive);
        Assert.Equal(TimeSpan.FromSeconds(30), received!.TimeToLive);
        Assert.Equal(received.EnqueuedTime + TimeSpan.FromSeconds(30), received.ExpiresAt);
    }

    [Fact]
    public async Task ExpiredMessage_IsDeadLetteredWithReason_NotDelivered()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("ttl-dlq") { DeadLetteringOnMessageExpiration = true });
        var sender = _client.CreateSender("ttl-dlq");
        await sender.SendMessageAsync(new ServiceBusMessage("expires") { TimeToLive = TimeSpan.FromSeconds(1) });
        await sender.SendMessageAsync(new ServiceBusMessage("stays"));
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        var receiver = _client.CreateReceiver("ttl-dlq");
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        var dead = await _client.CreateReceiver("ttl-dlq", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter })
            .ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("stays", first!.Body.ToString());
        Assert.Null(second);
        Assert.Equal("expires", dead!.Body.ToString());
        Assert.Equal("TTLExpiredException", dead.DeadLetterReason);
        Assert.Equal("The message expired and was dead lettered.", dead.DeadLetterErrorDescription);
        Assert.Null(dead.DeadLetterSource);
    }

    [Fact]
    public async Task ExpiredMessage_WithoutDeadLettering_IsDropped()
    {
        await _admin.CreateQueueAsync("ttl-drop");
        await _client.CreateSender("ttl-drop").SendMessageAsync(new ServiceBusMessage("x") { TimeToLive = TimeSpan.FromSeconds(1) });
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        Assert.Null(await _client.CreateReceiver("ttl-drop").ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        Assert.Null(await _client.CreateReceiver("ttl-drop", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter })
            .ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        var props = (await _admin.GetQueueRuntimePropertiesAsync("ttl-drop")).Value;
        Assert.Equal(0, props.ActiveMessageCount);
        Assert.Equal(0, props.DeadLetterMessageCount);
    }

    [Fact]
    public async Task ExpiredMessage_IsSweptWithoutAReceiver()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("ttl-sweep") { DeadLetteringOnMessageExpiration = true });
        await _client.CreateSender("ttl-sweep").SendMessageAsync(new ServiceBusMessage("x") { TimeToLive = TimeSpan.FromSeconds(1) });

        QueueRuntimeProperties props;
        var deadline = DateTime.UtcNow.AddSeconds(10);
        do
        {
            await Task.Delay(200);
            props = (await _admin.GetQueueRuntimePropertiesAsync("ttl-sweep")).Value;
        } while (props.DeadLetterMessageCount == 0 && DateTime.UtcNow < deadline);

        Assert.Equal(0, props.ActiveMessageCount);
        Assert.Equal(1, props.DeadLetterMessageCount);
    }

    [Fact]
    public async Task ExpiredSessionMessage_IsNotDelivered()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("ttl-session") { RequiresSession = true, DeadLetteringOnMessageExpiration = true });
        var sender = _client.CreateSender("ttl-session");
        await sender.SendMessageAsync(new ServiceBusMessage("expires") { SessionId = "s1", TimeToLive = TimeSpan.FromSeconds(1) });
        await sender.SendMessageAsync(new ServiceBusMessage("stays") { SessionId = "s1" });
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        await using var session = await _client.AcceptSessionAsync("ttl-session", "s1");
        var first = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var dead = await _client.CreateReceiver("ttl-session", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter })
            .ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("stays", first!.Body.ToString());
        Assert.Equal("expires", dead!.Body.ToString());
        Assert.Equal("TTLExpiredException", dead.DeadLetterReason);
    }

    [Fact]
    public async Task ExpiredMessage_IsForwardedToForwardDeadLetteredMessagesTo()
    {
        await _admin.CreateQueueAsync("ttl-fwd-target");
        await _admin.CreateQueueAsync(new CreateQueueOptions("ttl-fwd-source")
        {
            DeadLetteringOnMessageExpiration = true,
            ForwardDeadLetteredMessagesTo = "ttl-fwd-target",
        });
        await _client.CreateSender("ttl-fwd-source").SendMessageAsync(new ServiceBusMessage("x") { TimeToLive = TimeSpan.FromSeconds(2) });

        // The sweep forwards it once it expires. Its TTL restarts at the destination, so receive
        // straight away rather than waiting out another TTL.
        var forwarded = await _client.CreateReceiver("ttl-fwd-target").ReceiveMessageAsync(TimeSpan.FromSeconds(8));

        Assert.NotNull(forwarded);
        Assert.Equal("TTLExpiredException", forwarded.DeadLetterReason);
        Assert.Equal("The message expired and was dead lettered.", forwarded.DeadLetterErrorDescription);
        Assert.Equal("ttl-fwd-source", forwarded.DeadLetterSource);
    }

    [Fact]
    public async Task ExpiredDeferredMessage_IsNotFound_ThenDeadLettered()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("ttl-deferred") { DeadLetteringOnMessageExpiration = true });
        await _client.CreateSender("ttl-deferred").SendMessageAsync(new ServiceBusMessage("x") { TimeToLive = TimeSpan.FromSeconds(1) });
        var receiver = _client.CreateReceiver("ttl-deferred");
        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        await receiver.DeferMessageAsync(message!);
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => receiver.ReceiveDeferredMessageAsync(message!.SequenceNumber));

        Assert.Equal(ServiceBusFailureReason.MessageNotFound, ex.Reason);
        var dead = await _client.CreateReceiver("ttl-deferred", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter })
            .ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("TTLExpiredException", dead?.DeadLetterReason);
    }

    [Fact]
    public async Task ScheduledMessage_EnqueuedTimeIsActivationTime()
    {
        await _admin.CreateQueueAsync("ttl-scheduled");
        var sentAt = DateTimeOffset.UtcNow;
        var scheduledFor = sentAt.AddSeconds(2);
        await _client.CreateSender("ttl-scheduled").ScheduleMessageAsync(
            new ServiceBusMessage("x") { TimeToLive = TimeSpan.FromMinutes(1) }, scheduledFor);

        var received = await _client.CreateReceiver("ttl-scheduled").ReceiveMessageAsync(TimeSpan.FromSeconds(10));

        Assert.NotNull(received);
        Assert.True(received.EnqueuedTime >= scheduledFor.AddMilliseconds(-50), $"enqueued {received.EnqueuedTime:O}, scheduled {scheduledFor:O}");
        Assert.Equal(received.EnqueuedTime + TimeSpan.FromMinutes(1), received.ExpiresAt);
    }
}
