using System.Runtime.CompilerServices;
using AlmostServiceBus.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

// Resolve the emulator Host csproj path relative to THIS source file, so the
// sample works regardless of the current working directory.
var hostProject = Path.GetFullPath(
    Path.Combine(
        GetSourceDir(),
        "..", "..", "..",
        "src", "AlmostServiceBus.Host", "AlmostServiceBus.Host.csproj"));

var emulator = builder.AddServiceBusEmulator(
        name: "servicebus",
        hostProjectPath: hostProject,
        port: 5672,
        dashboardPort: 15672,
        disableTls: true);

builder.AddProject<Projects.OrderFlowDemo_OrderApi>("orderapi")
    .WithReference(emulator)
    .WaitFor(emulator)
    // Force Production so UseViteDevelopmentServer() is skipped and the pre-built
    // wwwroot/ is served as static files — no separate `npm run dev` needed.
    .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Production");

builder.AddProject<Projects.OrderFlowDemo_FulfillmentWorker>("fulfillment")
    .WithReference(emulator)
    .WaitFor(emulator);

await builder.Build().RunAsync();

static string GetSourceDir([CallerFilePath] string path = "") => Path.GetDirectoryName(path)!;
