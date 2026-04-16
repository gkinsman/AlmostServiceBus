using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Dashboard;
using AlmostServiceBus.Core.Hosting;
using AlmostServiceBus.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;
using System.Net.Sockets;

namespace AlmostServiceBus.TestHost;

public class ServiceBusEmulatorFixture : IAsyncDisposable
{
    private const int MaxStartAttempts = 10;
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromMilliseconds(100);

    private WebApplication? _webApp;
    private AmqpServer? _amqpServer;
    private TcpMultiplexer? _multiplexer;
    private CancellationTokenSource? _multiplexerCts;
    private Task? _multiplexerTask;
    private ScheduledMessageProcessor? _scheduledProcessor;
    private readonly MessageEventBus _eventBus = new();
    private readonly NamespaceRegistry _registry;
    private readonly string _namespace;
    private readonly int? _fixedPublicPort;

    public int PublicPort { get; private set; }
    internal int AmqpPort { get; private set; }
    internal int HttpPort { get; private set; }
    public string Namespace => _namespace;

    public string ConnectionString =>
        $"Endpoint=sb://localhost:{PublicPort};SharedAccessKeyName={_namespace};SharedAccessKey=emulator;UseDevelopmentEmulator=true";

    public string AmqpConnectionString =>
        $"amqp://localhost:{AmqpPort}";

    public ServiceBusEmulatorFixture(int? publicPort = null)
    {
        _namespace = $"test-{Guid.NewGuid():N}"[..20];
        _registry = new NamespaceRegistry(_eventBus);
        _fixedPublicPort = publicPort;
    }

    public async Task StartAsync()
    {
        for (var attempt = 1; attempt <= MaxStartAttempts; attempt++)
        {
            try
            {
                await StartCoreAsync();
                return;
            }
            catch (Exception ex) when (IsPortBindingFailure(ex) && attempt < MaxStartAttempts)
            {
                await CleanupAsync();
                await Task.Delay(TimeSpan.FromMilliseconds(InitialRetryDelay.TotalMilliseconds * attempt));
            }
            catch
            {
                await CleanupAsync();
                throw;
            }
        }
    }

    public async Task StopAsync()
    {
        await CleanupAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
    }

    public NamespaceContext GetNamespaceContext() => _registry.GetOrCreate(_namespace);

    public NamespaceContext GetDefaultNamespaceContext() => _registry.GetOrCreate("default");

    private async Task StartCoreAsync()
    {
        PublicPort = _fixedPublicPort ?? EmulatorInfrastructure.GetFreePort();
        AmqpPort = EmulatorInfrastructure.GetFreePort();
        HttpPort = EmulatorInfrastructure.GetFreePort();

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ListenLocalhost(HttpPort);
        });
        builder.Logging.ClearProviders();

        _webApp = builder.Build();
        _webApp.MapServiceBusManagementApi(_registry);
        _webApp.MapDashboardApi(_registry);
        _webApp.MapDashboardSse(_eventBus);
        await _webApp.StartAsync();

        _scheduledProcessor = new ScheduledMessageProcessor(_registry.GetOrCreate("default"));
        _scheduledProcessor.StartBackground(TimeSpan.FromMilliseconds(500));

        _amqpServer = new AmqpServer(new AmqpServerOptions { Port = AmqpPort }, _registry, _scheduledProcessor);
        _amqpServer.Start();

        _multiplexerCts = new CancellationTokenSource();
        _multiplexer = new TcpMultiplexer(PublicPort, AmqpPort, HttpPort);
        _multiplexerTask = _multiplexer.StartAsync(_multiplexerCts.Token);

        // Azure SDK's ServiceBusAdministrationClient connects to localhost:5300
        // for management when UseDevelopmentEmulator=true is set.
        // Only bind 5300 when a fixed public port was requested (MassTransit tests),
        // to avoid port collisions when multiple test projects run in parallel.
        if (_fixedPublicPort.HasValue)
        {
            try
            {
                var mgmtMultiplexer = new TcpMultiplexer(5300, AmqpPort, HttpPort);
                _ = mgmtMultiplexer.StartAsync(_multiplexerCts.Token);
            }
            catch { /* port 5300 may already be in use */ }
        }
    }

    private async Task CleanupAsync()
    {
        if (_multiplexerCts is not null)
        {
            try
            {
                await _multiplexerCts.CancelAsync();
            }
            catch
            {
                // Best effort during test teardown/retry.
            }
        }

        if (_multiplexerTask is not null)
        {
            try
            {
                await _multiplexerTask;
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        try
        {
            _amqpServer?.Stop();
        }
        catch
        {
            // Best effort during test teardown/retry.
        }

        _amqpServer?.Dispose();
        _amqpServer = null;

        _scheduledProcessor?.Dispose();
        _scheduledProcessor = null;

        if (_webApp is not null)
        {
            try
            {
                await _webApp.StopAsync();
            }
            catch
            {
                // Best effort during test teardown/retry.
            }

            await _webApp.DisposeAsync();
            _webApp = null;
        }

        _multiplexerTask = null;
        _multiplexer = null;
        _multiplexerCts?.Dispose();
        _multiplexerCts = null;
    }

    private static bool IsPortBindingFailure(Exception ex)
    {
        if (ex is SocketException { SocketErrorCode: SocketError.AddressAlreadyInUse })
            return true;

        if (ex.Message.Contains("Address already in use", StringComparison.OrdinalIgnoreCase))
            return true;

        return ex.InnerException is not null && IsPortBindingFailure(ex.InnerException);
    }
}
