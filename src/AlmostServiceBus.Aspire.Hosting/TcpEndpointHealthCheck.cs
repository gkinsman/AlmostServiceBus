using System.Net;
using System.Net.Sockets;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AlmostServiceBus.Aspire.Hosting;

/// <summary>
/// Health check that reports healthy once a TCP connection can be established to a
/// resource endpoint. The endpoint is resolved lazily at check-time, so it works with
/// Aspire's dynamically-allocated ports (the port isn't known at registration time).
///
/// For the emulator this gates dependent resources behind the AMQP listener actually
/// accepting connections — the multiplexer's listener is up only once the inner AMQP
/// server is started — so a consumer using <c>WaitFor</c> won't connect before the
/// emulator is ready and burn Azure SDK retry backoff.
/// </summary>
internal sealed class TcpEndpointHealthCheck : IHealthCheck
{
    private readonly Func<EndpointReference> _endpoint;

    public TcpEndpointHealthCheck(Func<EndpointReference> endpoint) => _endpoint = endpoint;

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var endpoint = _endpoint();
        if (!endpoint.IsAllocated)
            return HealthCheckResult.Unhealthy("Service Bus endpoint is not allocated yet.");

        var port = endpoint.Port;
        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy($"Cannot connect to localhost:{port}.", ex);
        }
    }
}
