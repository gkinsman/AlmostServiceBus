using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace AlmostServiceBus.Aspire.Hosting;

/// <summary>
/// Represents an Azure Service Bus Emulator resource in an Aspire application.
/// The emulator is launched as an executable (<c>dotnet run</c> against the Host project)
/// and exposes an AMQP/HTTPS endpoint plus an HTTP dashboard.
/// </summary>
public class AlmostServiceBusResource : ExecutableResource, IResourceWithConnectionString
{
    private readonly EndpointReference _serviceBusEndpoint;

    /// <summary>
    /// The port the dashboard is accessible on.
    /// </summary>
    public int DashboardPort { get; }

    /// <summary>
    /// When true, the emulator runs without TLS and the connection string carries
    /// <c>UseDevelopmentEmulator=true</c> so Azure.Messaging.ServiceBus connects
    /// over plain AMQP, matching the official Microsoft Service Bus emulator.
    /// </summary>
    public bool DisableTls { get; }

    /// <summary>
    /// Initialises a new <see cref="AlmostServiceBusResource"/>.
    /// </summary>
    /// <param name="name">The resource name.</param>
    /// <param name="workingDirectory">Working directory used when launching the executable.</param>
    /// <param name="dashboardPort">Port for the dashboard HTTP endpoint.</param>
    /// <param name="disableTls">When true, disables TLS and advertises the MS-emulator connection-string flag.</param>
    public AlmostServiceBusResource(string name, string workingDirectory, int dashboardPort, bool disableTls = false)
        : base(name, "dotnet", workingDirectory)
    {
        DashboardPort       = dashboardPort;
        DisableTls          = disableTls;
        _serviceBusEndpoint = new EndpointReference(this, "servicebus");
    }

    /// <summary>
    /// Gets the connection string expression for the emulator.
    /// Uses the allocated service-bus endpoint port so that Aspire port allocation is honoured.
    /// When <see cref="DisableTls"/> is true, appends <c>UseDevelopmentEmulator=true</c> so the
    /// Azure SDK connects over plain AMQP.
    /// </summary>
    public ReferenceExpression ConnectionStringExpression =>
        DisableTls
            ? ReferenceExpression.Create(
                $"Endpoint=sb://localhost:{_serviceBusEndpoint.Property(EndpointProperty.Port)};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true")
            : ReferenceExpression.Create(
                $"Endpoint=sb://localhost:{_serviceBusEndpoint.Property(EndpointProperty.Port)};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator");
}
