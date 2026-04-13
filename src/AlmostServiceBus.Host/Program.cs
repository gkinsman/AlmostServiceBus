using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Dashboard;
using AlmostServiceBus.Core.Hosting;
using AlmostServiceBus.Core.Management;
using Vite.AspNetCore;

// Enable AMQPNetLite frame tracing for diagnostic builds. Set TRACE_AMQP=1 env var.
if (Environment.GetEnvironmentVariable("TRACE_AMQP") == "1")
{
    Console.Error.WriteLine("[TRACE] Enabling AMQP frame tracing");
    Amqp.Trace.TraceLevel = Amqp.TraceLevel.Frame;
    Amqp.Trace.TraceListener = (level, format, args) =>
    {
        try
        {
            var line = args != null && args.Length > 0 ? string.Format(format, args) : format;
            Console.Error.WriteLine($"[AMQP] {line}");
        }
        catch (Exception ex) { Console.Error.WriteLine($"[AMQP-TRACE-ERR] {ex.Message}"); }
    };
}

// ── Management API server (internal, behind TLS multiplexer) ──

var mgmtBuilder = WebApplication.CreateBuilder(args);
mgmtBuilder.Logging.SetMinimumLevel(LogLevel.Warning);

// Wire up logging for AMQP components (not DI-managed)
AmqpLog.Factory = LoggerFactory.Create(b => b
    .SetMinimumLevel(mgmtBuilder.Configuration.GetValue("Logging:LogLevel:AlmostServiceBus.Amqp", LogLevel.Warning))
    .AddConsole());

var publicPort = mgmtBuilder.Configuration.GetValue("Port", 5672);
var dashboardPort = mgmtBuilder.Configuration.GetValue("DashboardPort", 15672);
var amqpsPort = 5671;
var internalHttpPort = EmulatorInfrastructure.GetFreePort();
var internalAmqpPort = EmulatorInfrastructure.GetFreePort();

var eventBus = new MessageEventBus();
var registry = new NamespaceRegistry(eventBus);

mgmtBuilder.WebHost.ConfigureKestrel(k =>
{
    k.ListenLocalhost(internalHttpPort);
});

var mgmtApp = mgmtBuilder.Build();
mgmtApp.MapServiceBusManagementApi(registry);
await mgmtApp.StartAsync();

// ── Dashboard server (separate port, no route conflicts) ──

var dashBuilder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
});
dashBuilder.Logging.SetMinimumLevel(LogLevel.Warning);
dashBuilder.Services.AddViteServices();
dashBuilder.Services.AddCors();

dashBuilder.WebHost.ConfigureKestrel(k =>
{
    k.ListenAnyIP(dashboardPort);
});

var dashApp = dashBuilder.Build();

dashApp.UseCors(policy => policy
    .AllowAnyOrigin()
    .AllowAnyMethod()
    .AllowAnyHeader());

if (dashApp.Environment.IsDevelopment())
{
    dashApp.UseViteDevelopmentServer();
}

dashApp.UseStaticFiles();
dashApp.MapDashboardApi(registry);
dashApp.MapDashboardSse(eventBus);
dashApp.MapFallbackToFile("index.html");

await dashApp.StartAsync();

// ── Scheduled message processor ──

var defaultContext = registry.GetOrCreate("default");
var scheduledProcessor = new ScheduledMessageProcessor(defaultContext);
scheduledProcessor.StartBackground(TimeSpan.FromMilliseconds(500));

// ── AMQP server ──

var amqpServer = new AmqpServer(new AmqpServerOptions { Port = internalAmqpPort }, registry, scheduledProcessor);
amqpServer.Start();

// ── TLS multiplexers ──

var cert = EmulatorInfrastructure.LoadDevCert();
var multiplexerCts = new CancellationTokenSource();

var multiplexer = new TcpMultiplexer(publicPort, internalAmqpPort, internalHttpPort, cert);
_ = multiplexer.StartAsync(multiplexerCts.Token);

if (amqpsPort != publicPort)
{
    var amqpsMultiplexer = new TcpMultiplexer(amqpsPort, internalAmqpPort, internalHttpPort, cert);
    _ = amqpsMultiplexer.StartAsync(multiplexerCts.Token);
}

// Microsoft emulator compatibility: management API on port 5300
var mgmtApiPort = 5300;
var mgmtMultiplexer = new TcpMultiplexer(mgmtApiPort, internalAmqpPort, internalHttpPort, cert);
_ = mgmtMultiplexer.StartAsync(multiplexerCts.Token);

// HTTPS on port 443 — the Azure SDK's FQDN-based admin client defaults here
// when using NamedKeyCredential (MassTransit's test infrastructure pattern)
try
{
    var httpsMultiplexer = new TcpMultiplexer(443, internalAmqpPort, internalHttpPort, cert);
    _ = httpsMultiplexer.StartAsync(multiplexerCts.Token);
}
catch (Exception ex)
{
    Console.WriteLine($"  Warning: Could not bind port 443 ({ex.Message}). FQDN-based admin clients may not work.");
}

// ── Shutdown ──

Console.WriteLine($"Azure Service Bus Emulator started");
Console.WriteLine($"  Service Bus: localhost:{publicPort} (HTTPS/AMQP), localhost:{amqpsPort} (AMQPS)");
Console.WriteLine($"  Management:  localhost:{mgmtApiPort} (HTTP), localhost:443 (HTTPS)");
Console.WriteLine($"  Dashboard:   http://localhost:{dashboardPort}");
Console.WriteLine();
Console.WriteLine($"  Connection String: Endpoint=sb://localhost:{publicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator");

// Block until Ctrl+C or process exit, then shut everything down quickly
var shutdownCts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; shutdownCts.Cancel(); };
AppDomain.CurrentDomain.ProcessExit += (_, _) => { shutdownCts.Cancel(); };

try { await Task.Delay(Timeout.Infinite, shutdownCts.Token); } catch (OperationCanceledException) { }

Console.WriteLine("Shutting down...");
multiplexerCts.Cancel();
scheduledProcessor.Dispose();
amqpServer.Stop();

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
await Task.WhenAll(
    mgmtApp.StopAsync(timeout.Token),
    dashApp.StopAsync(timeout.Token)
);

