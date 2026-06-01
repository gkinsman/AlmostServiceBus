using global::Amqp;
using global::Amqp.Listener;
using AlmostServiceBus.Core.Broker;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Wraps the AMQPNetLite <see cref="ConnectionListener"/> lifecycle.
/// Uses a custom <see cref="EmulatorContainer"/> instead of <see cref="ContainerHost"/>:
/// ContainerHost crashes on Attach frames with Coordinator targets, whereas the
/// custom container accepts them and drives AMQP transactions via a shared
/// <see cref="Broker.Transactions.TransactionManager"/>.
/// </summary>
public class AmqpServer : IDisposable
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<AmqpServer>();

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

        // One transaction manager shared across the whole server. Transaction ids are
        // globally-unique GUIDs, so a single table safely spans every connection and
        // entity — exactly what cross-entity transactions need.
        var transactions = new Broker.Transactions.TransactionManager();

        var container = new EmulatorContainer();
        container.SetNamespaceRegistry(_registry, _scheduledProcessor);
        container.SetTransactionManager(transactions);
        container.RegisterRequestProcessor("$cbs", new CbsRequestProcessor());
        container.RegisterRequestProcessor("$management", container.CreateManagementEndpoint(defaultContext, _scheduledProcessor));
        container.RegisterLinkProcessor(new ServiceBusLinkProcessor(_registry, _scheduledProcessor, transactions));

        _listener = new ConnectionListener(address, container);

        // Enable SASL so the Azure SDK's plain-AMQP connections (UseDevelopmentEmulator=true)
        // can authenticate. The SDK uses MSSBCBS (Microsoft Service Bus CBS) mechanism.
        _listener.SASL.EnableAnonymousMechanism = true;
        _listener.SASL.EnablePlainMechanism("RootManageSharedAccessKey", "emulator");
        _listener.SASL.EnableMechanism(MssbcbsSaslProfile.MechanismName, new MssbcbsSaslProfile());

        // Intercept outgoing deliveries to rewrite tags as GUIDs and handle connection cleanup.
        _listener.HandlerFactory = _ => new GuidDeliveryTagHandler();

        _listener.Open();
    }

    public void Stop()
    {
        try
        {
            _listener?.Close();
        }
        catch (ObjectDisposedException)
        {
            // Connection may already be closed by the remote peer during shutdown — ignore.
        }
        finally
        {
            _listener = null;
        }
    }

    public void Dispose()
    {
        Stop();
    }
}
