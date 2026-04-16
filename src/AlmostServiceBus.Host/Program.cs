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

// ── Management API server (behind the TCP multiplexer) ──

var mgmtBuilder = WebApplication.CreateBuilder(args);
mgmtBuilder.Logging.SetMinimumLevel(LogLevel.Warning);
mgmtBuilder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Warning);

// Wire up logging for AMQP components (not DI-managed)
AmqpLog.Factory = LoggerFactory.Create(b => b
    .SetMinimumLevel(mgmtBuilder.Configuration.GetValue("Logging:LogLevel:AlmostServiceBus.Amqp", LogLevel.Warning))
    .AddConsole());

var publicPort = mgmtBuilder.Configuration.GetValue("Port", 5672);
var dashboardPort = mgmtBuilder.Configuration.GetValue("DashboardPort", 15672);
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
dashBuilder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Warning);
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

// ── Connection multiplexers (plaintext — clients use UseDevelopmentEmulator=true) ──

var multiplexerCts = new CancellationTokenSource();

var multiplexer = new TcpMultiplexer(publicPort, internalAmqpPort, internalHttpPort);
_ = multiplexer.StartAsync(multiplexerCts.Token);

// Microsoft emulator compatibility: admin HTTP on port 5300
const int mgmtApiPort = 5300;
var mgmtMultiplexer = new TcpMultiplexer(mgmtApiPort, internalAmqpPort, internalHttpPort);
_ = mgmtMultiplexer.StartAsync(multiplexerCts.Token);

// ── Startup banner ──

const string cyan    = "\x1b[36m";
const string magenta = "\x1b[35m";
const string yellow  = "\x1b[33m";
const string green   = "\x1b[32m";
const string dim     = "\x1b[2m";
const string bold    = "\x1b[1m";
const string reset   = "\x1b[0m";

Console.WriteLine();
Console.WriteLine($"{magenta}   █████╗ ██╗     ███╗   ███╗ ██████╗ ███████╗████████╗{reset}");
Console.WriteLine($"{magenta}  ██╔══██╗██║     ████╗ ████║██╔═══██╗██╔════╝╚══██╔══╝{reset}");
Console.WriteLine($"{magenta}  ███████║██║     ██╔████╔██║██║   ██║███████╗   ██║   {reset}");
Console.WriteLine($"{cyan}  ██╔══██║██║     ██║╚██╔╝██║██║   ██║╚════██║   ██║   {reset}");
Console.WriteLine($"{cyan}  ██║  ██║███████╗██║ ╚═╝ ██║╚██████╔╝███████║   ██║   {reset}");
Console.WriteLine($"{cyan}  ╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝ ╚═════╝ ╚══════╝   ╚═╝   {reset}");
Console.WriteLine($"{bold}        S E R V I C E   B U S   E M U L A T O R{reset}");
Console.WriteLine();
Console.WriteLine($"  {green}●{reset} {bold}Service Bus{reset}  {dim}──▶{reset} localhost:{yellow}{publicPort}{reset} {dim}(plain AMQP/HTTP){reset}");
Console.WriteLine($"  {green}●{reset} {bold}Management {reset}  {dim}──▶{reset} localhost:{yellow}{mgmtApiPort}{reset} {dim}(plain HTTP){reset}");
Console.WriteLine($"  {green}●{reset} {bold}Dashboard  {reset}  {dim}──▶{reset} {cyan}http://localhost:{dashboardPort}{reset}");
Console.WriteLine();
var connStr = $"Endpoint=sb://localhost:{publicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
var boxInner = connStr.Length + 2;
const string label = " connection string ";
var topFill = new string('─', boxInner - label.Length - 1);
var botFill = new string('─', boxInner);
var padRight = new string(' ', boxInner - connStr.Length - 1);
Console.WriteLine($"  {dim}┌─{label}{topFill}┐{reset}");
Console.WriteLine($"  {dim}│{reset} Endpoint=sb://localhost:{yellow}{publicPort}{reset};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true{padRight}{dim}│{reset}");
Console.WriteLine($"  {dim}└{botFill}┘{reset}");
Console.WriteLine();
Console.WriteLine($"  {dim}press Ctrl+C to shut down{reset}");
Console.WriteLine();

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
