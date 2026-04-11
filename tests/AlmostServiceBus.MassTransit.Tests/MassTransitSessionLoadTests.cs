using System.Collections.Concurrent;
using AlmostServiceBus.TestHost;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;

namespace AlmostServiceBus.MassTransit.Tests;

public record SessionShipOrder(int Index, string Warehouse);

public class SessionShipOrderConsumer : IConsumer<SessionShipOrder>
{
    public static readonly ConcurrentBag<int> Consumed = [];
    public static int InvocationCount;

    public async Task Consume(ConsumeContext<SessionShipOrder> context)
    {
        Interlocked.Increment(ref InvocationCount);
        // Simulate processing work like the demo's ShipOrderConsumer
        await Task.Delay(Random.Shared.Next(10, 30));
        Consumed.Add(context.Message.Index);
    }
}

/// <summary>
/// Reproduces the MassTransit session queue load pattern from the OrderFlowDemo.
/// MassTransit + session queues + high message volume = the exact topology that breaks.
/// </summary>
public class MassTransitSessionLoadTests : IAsyncLifetime
{
    private const int TotalMessages = 500;
    private const int WarehouseCount = 10;

    private readonly ServiceBusEmulatorFixture _fixture = new(publicPort: 5672);
    private readonly string _queueName = $"session-mt-{Guid.NewGuid():N}"[..24];
    private ServiceProvider _provider = null!;
    private IBusControl _bus = null!;

    public async Task InitializeAsync()
    {
        SessionShipOrderConsumer.Consumed.Clear();
        SessionShipOrderConsumer.InvocationCount = 0;
        await _fixture.StartAsync();

        var services = new ServiceCollection();
        services.AddMassTransit(x =>
        {
            x.AddConsumer<SessionShipOrderConsumer>();

            x.UsingAzureServiceBus((ctx, cfg) =>
            {
                var cs = "Endpoint=sb://localhost:5672;" +
                         "SharedAccessKeyName=RootManageSharedAccessKey;" +
                         "SharedAccessKey=emulator;" +
                         "UseDevelopmentEmulator=true";
                cfg.Host(cs);

                cfg.ReceiveEndpoint(_queueName, e =>
                {
                    e.RequiresSession = true;
                    e.UseMessageRetry(r => r.None());
                    e.ConfigureConsumer<SessionShipOrderConsumer>(ctx);
                });
            });
        });

        _provider = services.BuildServiceProvider();
        _bus = _provider.GetRequiredService<IBusControl>();
        await _bus.StartAsync();

        // Wait for MassTransit to fully connect — the bus reports Ready
        // once all receive endpoints are consuming. Slow CI runners may
        // need more than a fixed delay.
        var health = _bus.CheckHealth();
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (health.Status != global::MassTransit.BusHealthStatus.Healthy && DateTime.UtcNow < deadline)
        {
            await Task.Delay(500);
            health = _bus.CheckHealth();
        }
        await Task.Delay(1000); // extra buffer for session receiver links
    }

    public async Task DisposeAsync()
    {
        try { await _bus.StopAsync(TimeSpan.FromSeconds(5)); } catch { }
        await _provider.DisposeAsync();
        await _fixture.DisposeAsync();
    }

    [Fact]
    public async Task SessionQueue_HighLoad_AllMessagesProcessed()
    {
        var sendEndpoint = await _bus.GetSendEndpoint(new Uri($"queue:{_queueName}"));

        // Send messages across many session IDs (warehouses)
        for (var i = 0; i < TotalMessages; i++)
        {
            var warehouse = $"warehouse-{i % WarehouseCount}";
            await sendEndpoint.Send(new SessionShipOrder(i, warehouse), ctx =>
            {
                ctx.SetSessionId(warehouse);
            });
        }

        // Wait for all messages to be consumed
        var deadline = DateTime.UtcNow.AddSeconds(120);
        while (DateTime.UtcNow < deadline)
        {
            if (SessionShipOrderConsumer.Consumed.Count >= TotalMessages)
                break;
            await Task.Delay(500);
        }

        var consumed = SessionShipOrderConsumer.Consumed.ToHashSet();

        Assert.True(consumed.Count >= TotalMessages,
            $"Only {consumed.Count}/{TotalMessages} messages consumed. " +
            $"Invocations: {SessionShipOrderConsumer.InvocationCount}");
    }

    /// <summary>
    /// Heavy load with sustained sending while consuming — messages keep arriving
    /// during processing, like the Black Friday scenario.
    /// </summary>
    [Fact]
    public async Task SessionQueue_SustainedBlackFriday_AllMessagesProcessed()
    {
        var sendEndpoint = await _bus.GetSendEndpoint(new Uri($"queue:{_queueName}"));
        const int total = 2000;

        // Send in waves with short pauses — sustained traffic pattern
        for (var i = 0; i < total; i++)
        {
            var warehouse = $"warehouse-{i % WarehouseCount}";
            await sendEndpoint.Send(new SessionShipOrder(i, warehouse), ctx =>
            {
                ctx.SetSessionId(warehouse);
            });

            // Every 50 messages, small pause to simulate sustained traffic
            if (i % 50 == 49)
                await Task.Delay(10);
        }

        var deadline = DateTime.UtcNow.AddSeconds(120);
        while (DateTime.UtcNow < deadline)
        {
            if (SessionShipOrderConsumer.Consumed.Count >= total)
                break;
            await Task.Delay(500);
        }

        var consumed = SessionShipOrderConsumer.Consumed.ToHashSet();

        Assert.True(consumed.Count >= total,
            $"Only {consumed.Count}/{total} messages consumed. " +
            $"Invocations: {SessionShipOrderConsumer.InvocationCount}");
    }
}
