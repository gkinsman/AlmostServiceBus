using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;

namespace AlmostServiceBus.Core.Hosting;

/// <summary>
/// Shared infrastructure utilities used by both the standalone host and the test fixture.
/// </summary>
public static class EmulatorInfrastructure
{
    [DllImport("libc", SetLastError = true)]
    private static extern int setenv(string name, string value, int overwrite);

    [DllImport("libc", SetLastError = true)]
    private static extern int unsetenv(string name);

    /// <summary>
    /// Process-wide lock around port allocation. Prevents the GetFreePort TOCTOU race
    /// (where two threads bind port 0, OS hands them the same port after both close
    /// their probe listener) within a single process. Cross-process races still need to
    /// be handled by the fixture's bind retry loop.
    /// </summary>
    private static readonly Lock s_portLock = new();

    /// <summary>
    /// Tracks ports recently handed out so that back-to-back GetFreePort calls within
    /// the same process don't return the same port (the OS can hand out a freshly-closed
    /// ephemeral port to the very next probe). Uses a small ring buffer; older entries
    /// fall out as the OS recycles ephemeral ports.
    /// </summary>
    private static readonly HashSet<int> s_recentlyAllocated = new();
    private const int MaxRecentlyAllocated = 64;

    /// <summary>
    /// Finds an available TCP port on the loopback interface. Serialized within a process
    /// to avoid the OS handing the same just-freed port to two concurrent callers.
    /// </summary>
    public static int GetFreePort()
    {
        lock (s_portLock)
        {
            // Try a few times — if the OS hands us a port we just gave out, probe again.
            for (var attempt = 0; attempt < 16; attempt++)
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                var port = ((IPEndPoint)listener.LocalEndpoint).Port;
                listener.Stop();

                if (!s_recentlyAllocated.Contains(port))
                {
                    if (s_recentlyAllocated.Count >= MaxRecentlyAllocated)
                    {
                        // Drop the oldest by clearing — the goal is just to spread
                        // allocations, not strict LRU.
                        s_recentlyAllocated.Clear();
                    }
                    s_recentlyAllocated.Add(port);
                    return port;
                }
            }

            // Fall back: just take whatever the OS gives, even if recently allocated.
            // The fixture's bind retry will handle the rare collision.
            var fallback = new TcpListener(IPAddress.Loopback, 0);
            fallback.Start();
            var fallbackPort = ((IPEndPoint)fallback.LocalEndpoint).Port;
            fallback.Stop();
            return fallbackPort;
        }
    }

    const string AspNetHttpsOid = "1.3.6.1.4.1.311.84.1.1";

    /// <summary>
    /// Loads the ASP.NET Core HTTPS development certificate from the current user's certificate store.
    /// Throws if no dev cert is found.
    /// </summary>
    public static X509Certificate2? LoadDevCert()
    {
        using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadOnly);
        var now = DateTime.Now;
        return store.Certificates
            .OfType<X509Certificate2>()
            .Where(c => c.Extensions.OfType<X509Extension>()
                .Any(e => e.Oid?.Value == AspNetHttpsOid))
            .Where(c => c.Subject == "CN=localhost")
            .Where(c => c.HasPrivateKey && c.NotBefore <= now && now <= c.NotAfter)
            .OrderByDescending(c => c.NotAfter)
            .FirstOrDefault();
    }

    private static int s_trustEnsured;

    /// <summary>
    /// Ensures the ASP.NET Core HTTPS development certificate is trusted by the process's
    /// TLS stack, so the Azure SDK's AMQPS client accepts it without client-side
    /// validation overrides.
    ///
    /// Two layers:
    ///
    /// 1. Shell out to <c>dotnet dev-certs https --trust</c>. On Windows/macOS this is
    ///    sufficient — the cert ends up in the per-user trust store that the platform's
    ///    TLS stack consults automatically. On Linux it updates <c>~/.aspnet/dev-certs/trust/</c>
    ///    (and, on newer distros with NSS tooling, some browser trust dbs), but doesn't
    ///    touch the system CA bundle that OpenSSL-based stacks read.
    ///
    /// 2. On Linux, point OpenSSL at <c>~/.aspnet/dev-certs/trust/</c> via
    ///    <c>SSL_CERT_DIR</c>, and unset <c>SSL_CERT_FILE</c> (which overrides
    ///    <c>SSL_CERT_DIR</c> when both are set). We do this with native
    ///    <c>setenv(3)</c>/<c>unsetenv(3)</c> because <see cref="Environment.SetEnvironmentVariable"/>
    ///    only updates the managed copy of the environment; OpenSSL reads the native
    ///    <c>getenv(3)</c> view.
    ///
    /// CI environments that set up system trust themselves (e.g. via
    /// <c>sudo update-ca-certificates</c>) don't need any of this — the CLI is
    /// idempotent when the cert is already trusted, and the env-var step short-circuits
    /// when <c>~/.aspnet/dev-certs/trust/</c> doesn't exist.
    /// </summary>
    public static void EnsureDevCertTrusted()
    {
        // Only attempt once per process — the CLI takes a second or two even when the
        // cert is already trusted.
        if (Interlocked.Exchange(ref s_trustEnsured, 1) != 0)
            return;

        // Step 1: let the platform CLI handle trust wherever it can.
        try
        {
            using var proc = Process.Start(new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = "dev-certs https --trust",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });

            if (proc is not null && !proc.WaitForExit(TimeSpan.FromSeconds(10)))
            {
                try { proc.Kill(entireProcessTree: true); } catch { }
            }
        }
        catch
        {
            // `dotnet` may not be on PATH in packaged scenarios; fall through.
        }

        // Step 2: Linux-only — bridge the dev-certs trust dir into OpenSSL's search path.
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            return;

        var trustDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".aspnet", "dev-certs", "trust");

        if (!Directory.Exists(trustDir))
            return;

        // SSL_CERT_FILE overrides SSL_CERT_DIR in OpenSSL. If set, unset it so
        // SSL_CERT_DIR takes effect.
        var sslCertFile = Environment.GetEnvironmentVariable("SSL_CERT_FILE");
        if (!string.IsNullOrEmpty(sslCertFile))
        {
            Environment.SetEnvironmentVariable("SSL_CERT_FILE", null);
            unsetenv("SSL_CERT_FILE");
        }

        var current = Environment.GetEnvironmentVariable("SSL_CERT_DIR") ?? "";
        if (!current.Contains(trustDir))
        {
            var systemCerts = "/usr/lib/ssl/certs";
            var newValue = string.IsNullOrEmpty(current)
                ? $"{trustDir}:{systemCerts}"
                : $"{trustDir}:{current}";
            Environment.SetEnvironmentVariable("SSL_CERT_DIR", newValue);
            setenv("SSL_CERT_DIR", newValue, 1);
        }
    }
}
