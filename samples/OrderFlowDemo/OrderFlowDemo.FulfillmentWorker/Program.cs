using MassTransit;
using OrderFlowDemo.FulfillmentWorker.Consumers;
using OrderFlowDemo.ServiceDefaults;

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();

builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<ReserveInventoryConsumer>();
    x.AddConsumer<PickOrderConsumer>();
    x.AddConsumer<ShipOrderConsumer>();
    x.AddConsumer<OrderShippedConsumer>();

    x.AddInMemoryInboxOutbox();

    x.UsingAzureServiceBus((context, cfg) =>
    {
        cfg.Host(builder.Configuration.GetConnectionString("servicebus"));

        // ShipOrderConsumer throws on a simulated carrier rejection and logs "will retry".
        // Without a retry policy MassTransit would fault the message straight to the
        // logistics-dispatch_error queue and the order would sit in Shipping forever.
        cfg.UseMessageRetry(r => r.Intervals(500, 1000, 2000));

        // Configure session queue for logistics dispatch (FIFO per warehouse)
        cfg.ReceiveEndpoint("logistics-dispatch", e =>
        {
            e.RequiresSession = true;
            e.ConfigureConsumer<ShipOrderConsumer>(context);
        });

        cfg.ConfigureEndpoints(context);
    });
});

var host = builder.Build();
host.Run();
