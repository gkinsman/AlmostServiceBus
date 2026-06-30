using System.Transactions;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Verifies AMQP transaction support through the real Azure.Messaging.ServiceBus
/// SDK driving a <see cref="TransactionScope"/> against the running emulator.
/// This is the acceptance bar for the feature: commit applies every operation,
/// rollback applies none — including across entities
/// (<see cref="ServiceBusClientOptions.EnableCrossEntityTransactions"/>).
/// </summary>
public class TransactionTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    private ServiceBusClient CreateClient(bool crossEntity = false)
    {
        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
        return new ServiceBusClient(cs, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            EnableCrossEntityTransactions = crossEntity,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) }
        });
    }

    [Fact]
    public async Task Commit_MakesTransactionalSendVisible()
    {
        var ctx = _fixture.GetDefaultNamespaceContext();
        ctx.CreateQueue("txn-send-commit");

        await using var client = CreateClient();
        var sender = client.CreateSender("txn-send-commit");

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("hello") { MessageId = "m1" });
            scope.Complete();
        }

        var receiver = client.CreateReceiver("txn-send-commit");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(received);
        Assert.Equal("m1", received!.MessageId);
    }

    [Fact]
    public async Task Rollback_DiscardsTransactionalSend()
    {
        var ctx = _fixture.GetDefaultNamespaceContext();
        ctx.CreateQueue("txn-send-rollback");

        await using var client = CreateClient();
        var sender = client.CreateSender("txn-send-rollback");

        // Scope is disposed WITHOUT Complete() → the SDK discharges with fail=true.
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("nope") { MessageId = "m1" });
            // no scope.Complete()
        }

        var receiver = client.CreateReceiver("txn-send-rollback");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));

        Assert.Null(received);
    }

    [Fact]
    public async Task Commit_AppliesCompleteAndSend_AcrossEntities()
    {
        var ctx = _fixture.GetDefaultNamespaceContext();
        var src = ctx.CreateQueue("txn-src-commit");
        ctx.CreateQueue("txn-dst-commit");

        await using var seedClient = CreateClient();
        var seedSender = seedClient.CreateSender("txn-src-commit");
        await seedSender.SendMessageAsync(new ServiceBusMessage("payload") { MessageId = "orig" });

        await using var client = CreateClient(crossEntity: true);
        // Receiver first: with cross-entity transactions the first entity anchors the connection.
        var receiver = client.CreateReceiver("txn-src-commit");
        var dstSender = client.CreateSender("txn-dst-commit");

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await dstSender.SendMessageAsync(new ServiceBusMessage("replayed") { MessageId = "copy" });
            await receiver.CompleteMessageAsync(msg!);
            scope.Complete();
        }

        // The replayed copy landed on the destination. Verify with the plain (non-cross-entity)
        // seedClient: a cross-entity client is pinned to the source entity, so opening a receiver
        // on a *different* entity through `client` is rejected by real Azure Service Bus
        // ("Local transactions cannot span multiple top-level entities").
        var dstReceiver = seedClient.CreateReceiver("txn-dst-commit");
        var copy = await dstReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(copy);
        Assert.Equal("copy", copy!.MessageId);

        // The original was completed (consumed) on the source.
        Assert.Equal(1, src.ConsumedCount);
    }

    [Fact]
    public async Task Rollback_LeavesCompleteAndSendUnapplied_AcrossEntities()
    {
        var ctx = _fixture.GetDefaultNamespaceContext();
        var src = ctx.CreateQueue("txn-src-rollback");
        src.LockDuration = TimeSpan.FromSeconds(2); // make redelivery fast and observable
        ctx.CreateQueue("txn-dst-rollback");

        await using var seedClient = CreateClient();
        var seedSender = seedClient.CreateSender("txn-src-rollback");
        await seedSender.SendMessageAsync(new ServiceBusMessage("payload") { MessageId = "orig" });

        await using var client = CreateClient(crossEntity: true);
        var receiver = client.CreateReceiver("txn-src-rollback");
        var dstSender = client.CreateSender("txn-dst-rollback");

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await dstSender.SendMessageAsync(new ServiceBusMessage("replayed") { MessageId = "copy" });
            await receiver.CompleteMessageAsync(msg!);
            // no scope.Complete() → rollback
        }

        // The send was discarded — destination stays empty. Verify with the plain (non-cross-entity)
        // seedClient: the cross-entity `client` is pinned to the source entity, so a receiver on a
        // different entity through it is rejected by real Azure Service Bus.
        var dstReceiver = seedClient.CreateReceiver("txn-dst-rollback");
        var copy = await dstReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(copy);

        // The complete was discarded — the original was never consumed.
        Assert.Equal(0, src.ConsumedCount);
    }
}
