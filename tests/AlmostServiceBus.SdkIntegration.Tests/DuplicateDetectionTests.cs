using System.Net;
using System.Net.Sockets;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

// The MassTransit delayed-redelivery shape: a message forwarded into a duplicate-detection queue,
// then rescheduled with the same MessageId. ASB documents that the scheduled duplicate is dropped.
public class DuplicateDetectionTests : IAsyncLifetime
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
    public async Task RescheduledForwardedMessageWithSameMessageId_IsDropped()
    {
        await _admin.CreateQueueAsync(new CreateQueueOptions("dedup-input")
        {
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromSeconds(30),
        });
        await _admin.CreateTopicAsync("dedup-topic");
        await _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("dedup-topic", "fwd") { ForwardTo = "dedup-input" });

        await _client.CreateSender("dedup-topic").SendMessageAsync(new ServiceBusMessage("original") { MessageId = "m1" });

        var receiver = _client.CreateReceiver("dedup-input");
        var original = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(original);
        await receiver.CompleteMessageAsync(original!);

        var sender = _client.CreateSender("dedup-input");
        await sender.ScheduleMessageAsync(new ServiceBusMessage("redelivery") { MessageId = "m1" }, DateTimeOffset.UtcNow.AddMilliseconds(100));
        await sender.ScheduleMessageAsync(new ServiceBusMessage("fresh") { MessageId = "m2" }, DateTimeOffset.UtcNow.AddMilliseconds(100));

        var next = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("fresh", next?.Body.ToString());
        await receiver.CompleteMessageAsync(next!);
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2)));
    }
}
