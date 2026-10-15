using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Aspire.Hosting;
using AlmostServiceBus.Aspire.Hosting;

namespace AlmostServiceBus.Aspire.Tests;

public class HostShutdownTests
{
    // On Linux and macOS Aspire stops an executable resource with SIGTERM. A Host that ignores it
    // outlives the AppHost and keeps its ports, memory and CPU until someone kills it by hand.
    [Fact]
    public async Task Host_launched_by_AddServiceBusEmulator_exits_on_SIGTERM()
    {
        if (OperatingSystem.IsWindows())
            return;

        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder.AddServiceBusEmulator("servicebus").Resource;
        var hostDll = Path.Combine(resource.WorkingDirectory, "AlmostServiceBus.Host.dll");

        var psi = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var arg in new[] { "exec", hostDll, "--Port", GetFreePort().ToString(), "--DashboardPort", "0" })
            psi.ArgumentList.Add(arg);

        using var host = Process.Start(psi)!;
        try
        {
            var ready = new TaskCompletionSource();
            host.OutputDataReceived += (_, e) =>
            {
                if (e.Data?.Contains("press Ctrl+C to shut down") == true)
                    ready.TrySetResult();
            };
            host.ErrorDataReceived += (_, _) => { };
            host.BeginOutputReadLine();
            host.BeginErrorReadLine();

            await ready.Task.WaitAsync(TimeSpan.FromSeconds(60));

            using (var kill = Process.Start("kill", ["-TERM", host.Id.ToString()]))
                await kill.WaitForExitAsync();

            using var exitTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            try
            {
                await host.WaitForExitAsync(exitTimeout.Token);
            }
            catch (OperationCanceledException)
            {
                Assert.Fail("Host was still running 15s after SIGTERM");
            }
        }
        finally
        {
            if (!host.HasExited)
                host.Kill(entireProcessTree: true);
        }
    }

    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
