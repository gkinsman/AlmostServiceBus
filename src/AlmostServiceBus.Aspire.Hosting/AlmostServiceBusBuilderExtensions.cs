using System.Diagnostics;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;

namespace AlmostServiceBus.Aspire.Hosting;

/// <summary>
/// Extension methods for adding AlmostServiceBus to an Aspire distributed application.
/// </summary>
public static class AlmostServiceBusBuilderExtensions
{
    /// <summary>
    /// Adds AlmostServiceBus as an executable resource.
    /// The emulator is launched via <c>dotnet exec</c> using the Host binary
    /// embedded in this NuGet package.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The resource name (used in the Aspire dashboard and for <c>WithReference</c>).</param>
    /// <param name="port">
    /// The port the emulator listens on for AMQP traffic.
    /// When <c>null</c>, Aspire assigns a free port automatically.
    /// </param>
    /// <param name="dashboardPort">
    /// The port the emulator dashboard listens on.
    /// Defaults to <c>15672</c>.
    /// </param>
    /// <returns>A resource builder that can be further configured.</returns>
    public static IResourceBuilder<AlmostServiceBusResource> AddServiceBusEmulator(
        this IDistributedApplicationBuilder builder,
        string name,
        int? port = null,
        int dashboardPort = 15672)
    {
        var hostDll = ResolveHostDll();

        var resource = new AlmostServiceBusResource(name, Path.GetDirectoryName(hostDll)!, dashboardPort);

        // No --no-launch-profile here. That is a `dotnet run` flag; `dotnet exec` hands it to the
        // Host, whose command-line parser pairs it with the "--Port" that follows and drops the
        // port value. The Host then listened on its default port while the connection string
        // advertised the requested one, so a non-default port never worked.
        var args = new List<object>
        {
            "exec",
            hostDll,
        };

        var resourceBuilder = builder.AddResource(resource)
            .WithArgs(args.ToArray())
            .WithEndpoint(port ?? 5672, name: "servicebus", scheme: "tcp", isProxied: false)
            .WithEndpoint(dashboardPort, dashboardPort, name: "dashboard", scheme: "http", isProxied: false)
            .WithExternalHttpEndpoints();

        // Pass the allocated port to the Host as --Port
        resourceBuilder = resourceBuilder.WithArgs(context =>
        {
            var serviceBusEndpoint = resource.GetEndpoint("servicebus");
            context.Args.Add("--Port");
            context.Args.Add(serviceBusEndpoint.Property(EndpointProperty.Port));

            context.Args.Add("--DashboardPort");
            context.Args.Add(dashboardPort.ToString());
        });

        return WithServiceBusReadinessCheck(builder, resourceBuilder, resource, name);
    }

    /// <summary>
    /// Adds AlmostServiceBus as an executable resource, running a Host project from source.
    /// Use this overload when developing against the emulator source code.
    /// </summary>
    /// <param name="builder">The distributed application builder.</param>
    /// <param name="name">The resource name.</param>
    /// <param name="hostProjectPath">Absolute or relative path to <c>AlmostServiceBus.Host.csproj</c>.</param>
    /// <param name="port">The emulator port. Defaults to <c>5672</c>.</param>
    /// <param name="dashboardPort">The dashboard port. Defaults to <c>15672</c>.</param>
    public static IResourceBuilder<AlmostServiceBusResource> AddServiceBusEmulator(
        this IDistributedApplicationBuilder builder,
        string name,
        string hostProjectPath,
        int port = 5672,
        int dashboardPort = 15672)
    {
        var resource = new AlmostServiceBusResource(name, ".", dashboardPort);

        var args = new List<object>
        {
            "run",
            "--project",
            hostProjectPath,
            "--no-launch-profile",
        };

        var resourceBuilder = builder.AddResource(resource)
            .WithArgs(args.ToArray())
            .WithEndpoint(port, port, name: "servicebus", scheme: "tcp", isProxied: false)
            .WithEndpoint(dashboardPort, dashboardPort, name: "dashboard", scheme: "http", isProxied: false)
            .WithExternalHttpEndpoints();

        resourceBuilder = resourceBuilder.WithArgs(context =>
        {
            var serviceBusEndpoint = resource.GetEndpoint("servicebus");
            context.Args.Add("--Port");
            context.Args.Add(serviceBusEndpoint.Property(EndpointProperty.Port));

            context.Args.Add("--DashboardPort");
            context.Args.Add(dashboardPort.ToString());
        });

        return WithServiceBusReadinessCheck(builder, resourceBuilder, resource, name);
    }

    /// <summary>
    /// Enables the emulator's opt-in HTTPS admin (entity-management) endpoint and serves it with the
    /// ASP.NET Core HTTPS development certificate — the same cert Aspire uses for its own HTTPS
    /// endpoints. This lets the Node/Python/Java admin SDKs, which hard-code HTTPS, manage entities
    /// against the emulator. The .NET admin client does not need this — it uses the plaintext admin
    /// endpoint on port 5300.
    /// </summary>
    /// <param name="builder">The emulator resource builder.</param>
    /// <param name="port">The HTTPS admin port. Defaults to <c>5301</c>.</param>
    /// <param name="certPath">
    /// Optional path to a PFX/PEM certificate to serve instead of the ASP.NET Core dev cert.
    /// When <c>null</c>, the dev cert is exported and used.
    /// </param>
    /// <param name="certPassword">The password for <paramref name="certPath"/>, if any.</param>
    /// <remarks>
    /// The dev cert's SAN is <c>localhost</c>, so clients must reach the emulator as <c>localhost</c>.
    /// Node/Python/Java clients still need to trust the cert explicitly (the emulator writes it to
    /// <c>emulator-ca.crt</c> / <c>emulator-truststore.p12</c> in its cert directory); see
    /// <c>certs/README.md</c>. The Java admin SDK additionally only reaches the endpoint on port 443.
    /// </remarks>
    public static IResourceBuilder<AlmostServiceBusResource> WithAdminTls(
        this IResourceBuilder<AlmostServiceBusResource> builder,
        int port = 5301,
        string? certPath = null,
        string? certPassword = null)
    {
        builder = builder.WithEndpoint(port, port, name: "admintls", scheme: "https", isProxied: false);

        var resolvedCertPath = certPath;
        var resolvedPassword = certPassword;

        // No BYO cert supplied: export the ASP.NET Core HTTPS dev cert so the emulator can serve it.
        if (resolvedCertPath is null)
        {
            resolvedPassword = Guid.NewGuid().ToString("N");
            resolvedCertPath = ExportDevCert(builder.Resource.Name, resolvedPassword);
        }

        return builder.WithArgs(context =>
        {
            context.Args.Add("--AdminTlsEnabled");
            context.Args.Add("true");
            context.Args.Add("--AdminTlsPort");
            context.Args.Add(port.ToString());

            if (resolvedCertPath is not null)
            {
                context.Args.Add("--AdminTlsCertPath");
                context.Args.Add(resolvedCertPath);
            }

            if (!string.IsNullOrEmpty(resolvedPassword))
            {
                context.Args.Add("--AdminTlsCertPassword");
                context.Args.Add(resolvedPassword);
            }
        });
    }

    /// <summary>
    /// Exports the ASP.NET Core HTTPS development certificate to a PFX the emulator can load with
    /// Kestrel. Throws if no dev cert is available (run <c>dotnet dev-certs https --trust</c> first).
    /// </summary>
    private static string ExportDevCert(string resourceName, string password)
    {
        var dir = Path.Combine(Path.GetTempPath(), "almostservicebus-aspire", resourceName);
        Directory.CreateDirectory(dir);
        var pfxPath = Path.Combine(dir, "admin-devcert.pfx");
        if (File.Exists(pfxPath))
            File.Delete(pfxPath);

        var psi = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("dev-certs");
        psi.ArgumentList.Add("https");
        psi.ArgumentList.Add("--export-path");
        psi.ArgumentList.Add(pfxPath);
        psi.ArgumentList.Add("--password");
        psi.ArgumentList.Add(password);
        psi.ArgumentList.Add("--format");
        psi.ArgumentList.Add("Pfx");

        using var proc = Process.Start(psi)
            ?? throw new InvalidOperationException("Failed to start 'dotnet dev-certs' to export the HTTPS development certificate.");

        var stdout = proc.StandardOutput.ReadToEnd();
        var stderr = proc.StandardError.ReadToEnd();
        proc.WaitForExit();

        if (proc.ExitCode != 0 || !File.Exists(pfxPath))
        {
            throw new InvalidOperationException(
                $"'dotnet dev-certs https --export-path' failed (exit {proc.ExitCode}). " +
                "Ensure an HTTPS development certificate exists by running 'dotnet dev-certs https --trust'. " +
                $"{stdout}{stderr}".Trim());
        }

        return pfxPath;
    }

    /// <summary>
    /// Registers a TCP health check against the emulator's <c>servicebus</c> endpoint and
    /// attaches it to the resource, so consumers can gate startup with <c>WaitFor</c> until
    /// the AMQP listener is actually accepting connections. The endpoint port is resolved
    /// lazily, honouring Aspire's dynamic port allocation.
    /// </summary>
    private static IResourceBuilder<AlmostServiceBusResource> WithServiceBusReadinessCheck(
        IDistributedApplicationBuilder builder,
        IResourceBuilder<AlmostServiceBusResource> resourceBuilder,
        AlmostServiceBusResource resource,
        string name)
    {
        var healthCheckKey = $"{name}_servicebus_tcp";

        builder.Services.AddHealthChecks().AddCheck(
            healthCheckKey,
            new TcpEndpointHealthCheck(() => resource.GetEndpoint("servicebus")));

        return resourceBuilder.WithHealthCheck(healthCheckKey);
    }

    /// <summary>
    /// Locates the embedded Host DLL. Search order:
    /// 1. Output directory: almostservicebus-host/ subdirectory (copied by .targets file from NuGet package)
    /// 2. Local dev: obj/host-publish/ relative to the Aspire.Hosting project source
    /// </summary>
    private static string ResolveHostDll()
    {
        const string hostDllName = "AlmostServiceBus.Host.dll";
        const string hostSubDir = "almostservicebus-host";

        // Primary: .targets file copies tools/ → almostservicebus-host/ in the output directory
        var baseDir = AppContext.BaseDirectory;
        var outputPath = Path.Combine(baseDir, hostSubDir, hostDllName);
        if (File.Exists(outputPath))
            return Path.GetFullPath(outputPath);

        // Local dev: Host published to obj/host-publish/ during build
        var current = new DirectoryInfo(baseDir);
        while (current is not null)
        {
            var candidate = Path.Combine(current.FullName, "src", "AlmostServiceBus.Aspire.Hosting", "obj", "host-publish", hostDllName);
            if (File.Exists(candidate))
                return Path.GetFullPath(candidate);
            current = current.Parent;
        }

        throw new InvalidOperationException(
            $"Could not locate {hostDllName}. If consuming via NuGet, ensure the AlmostServiceBus.Aspire.Hosting package is installed correctly. " +
            "If building from source, use the overload that takes a hostProjectPath parameter.");
    }
}
