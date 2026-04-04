using global::Amqp;
using global::Amqp.Listener;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Wraps the AMQPNetLite <see cref="ConnectionListener"/> lifecycle.
/// Uses a custom <see cref="EmulatorContainer"/> instead of <see cref="ContainerHost"/>
/// to avoid a crash when clients send Attach frames with Coordinator targets
/// (used for AMQP transactions by NServiceBus and others).
/// </summary>
public class AmqpServer : IDisposable
{
    private readonly AmqpServerOptions _options;
    private readonly NamespaceRegistry _registry;
    private readonly ScheduledMessageProcessor? _scheduledProcessor;
    private ConnectionListener? _listener;

    public AmqpServer(AmqpServerOptions options, NamespaceRegistry registry, ScheduledMessageProcessor? scheduledProcessor = null)
    {
        _options = options;
        _registry = registry;
        _scheduledProcessor = scheduledProcessor;
    }

    public void Start()
    {
        var address = new Address(_options.Host, _options.Port, null, null, "/", "AMQP");

        // Build the custom container that handles Coordinator targets gracefully.
        var defaultContext = _registry.GetOrCreate("default");

        var container = new EmulatorContainer();
        container.SetNamespaceRegistry(_registry, _scheduledProcessor);
        container.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());
        container.RegisterRequestProcessor("$management", container.CreateManagementEndpoint(defaultContext, _scheduledProcessor));
        container.RegisterLinkProcessor(new ServiceBusLinkProcessor(_registry, _scheduledProcessor));

        _listener = new ConnectionListener(address, container);

        // Enable SASL so the Azure SDK's AMQPS connections can authenticate.
        // The SDK uses MSSBCBS (Microsoft Service Bus CBS) mechanism.
        _listener.SASL.EnableAnonymousMechanism = true;
        _listener.SASL.EnablePlainMechanism("RootManageSharedAccessKey", "emulator");
        _listener.SASL.EnableMechanism(MssbcbsSaslProfile.MechanismName, new MssbcbsSaslProfile());

        // Intercept outgoing deliveries to rewrite tags as GUIDs and handle connection cleanup.
        _listener.HandlerFactory = _ => new GuidDeliveryTagHandler();

        _listener.Open();
    }

    public void Stop()
    {
        _listener?.Close();
        _listener = null;
    }

    public void Dispose()
    {
        Stop();
    }
}
