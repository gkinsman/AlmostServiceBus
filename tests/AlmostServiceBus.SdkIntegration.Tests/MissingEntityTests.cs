using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

public class MissingEntityTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    private ServiceBusClient CreateClient() => new(
        _fixture.ConnectionString,
        new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
        });

    [Fact]
    public async Task SendToMissingEntity_ThrowsMessagingEntityNotFound()
    {
        await using var client = CreateClient();
        var sender = client.CreateSender("no-such-entity");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            () => sender.SendMessageAsync(new ServiceBusMessage("hello")));

        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
        Assert.Null(_fixture.GetNamespaceContext().GetQueue("no-such-entity"));
    }

    [Fact]
    public async Task CreateBatchForMissingEntity_ThrowsMessagingEntityNotFound()
    {
        await using var client = CreateClient();
        var sender = client.CreateSender("no-such-entity");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            async () => await sender.CreateMessageBatchAsync());

        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact]
    public async Task ScheduleToMissingEntity_ThrowsMessagingEntityNotFound()
    {
        await using var client = CreateClient();
        var sender = client.CreateSender("no-such-entity");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            () => sender.ScheduleMessageAsync(new ServiceBusMessage("later"), DateTimeOffset.UtcNow.AddMinutes(5)));

        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact]
    public async Task ReceiveFromMissingQueue_ThrowsMessagingEntityNotFound()
    {
        await using var client = CreateClient();
        await using var receiver = client.CreateReceiver("no-such-entity");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            () => receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2)));

        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact]
    public async Task ReceiveFromMissingSubscription_ThrowsMessagingEntityNotFound()
    {
        _fixture.GetNamespaceContext().CreateTopic("real-topic");
        await using var client = CreateClient();
        await using var receiver = client.CreateReceiver("real-topic", "no-such-subscription");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(
            () => receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2)));

        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }

    [Fact]
    public async Task SendToMissingEntity_DoesNotBreakOtherSendersOnTheClient()
    {
        _fixture.GetNamespaceContext().CreateQueue("real-queue");
        await using var client = CreateClient();

        await Assert.ThrowsAsync<ServiceBusException>(
            () => client.CreateSender("no-such-entity").SendMessageAsync(new ServiceBusMessage("lost")));

        await client.CreateSender("real-queue").SendMessageAsync(new ServiceBusMessage("kept"));
        await using var receiver = client.CreateReceiver("real-queue");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("kept", received?.Body.ToString());
    }
}
