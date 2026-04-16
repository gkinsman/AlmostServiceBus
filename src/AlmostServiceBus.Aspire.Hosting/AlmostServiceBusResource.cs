using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace AlmostServiceBus.Aspire.Hosting;

/// <summary>
/// Represents an Azure Service Bus Emulator resource in an Aspire application.
/// The emulator is launched as an executable (<c>dotnet run</c> against the Host project)
/// and exposes a plain AMQP/HTTP endpoint plus an HTTP dashboard. Clients connect via
/// the emitted connection string, which carries <c>UseDevelopmentEmulator=true</c> so
/// <c>Azure.Messaging.ServiceBus</c> uses plain AMQP — matching Microsoft's official
/// Service Bus emulator behaviour.
/// </summary>
public class AlmostServiceBusResource : ExecutableResource, IResourceWithConnectionString
{
    private readonly EndpointReference _serviceBusEndpoint;

    /// <summary>
    /// The port the dashboard is accessible on.
    /// </summary>
    public int DashboardPort { get; }

    /// <summary>
    /// Initialises a new <see cref="AlmostServiceBusResource"/>.
    /// </summary>
    /// <param name="name">The resource name.</param>
    /// <param name="workingDirectory">Working directory used when launching the executable.</param>
    /// <param name="dashboardPort">Port for the dashboard HTTP endpoint.</param>
    public AlmostServiceBusResource(string name, string workingDirectory, int dashboardPort)
        : base(name, "dotnet", workingDirectory)
    {
        DashboardPort       = dashboardPort;
        _serviceBusEndpoint = new EndpointReference(this, "servicebus");
    }

    /// <summary>
    /// Gets the connection string expression for the emulator. Uses the allocated
    /// service-bus endpoint port so Aspire port allocation is honoured, and carries
    /// <c>UseDevelopmentEmulator=true</c> so the Azure SDK connects over plain AMQP.
    /// </summary>
    public ReferenceExpression ConnectionStringExpression =>
        ReferenceExpression.Create(
            $"Endpoint=sb://localhost:{_serviceBusEndpoint.Property(EndpointProperty.Port)};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true");
}
