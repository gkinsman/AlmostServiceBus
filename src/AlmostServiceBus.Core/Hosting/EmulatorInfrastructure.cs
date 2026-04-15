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

    /// <summary>
    /// On Linux, ensures the ASP.NET dev cert is in SSL_CERT_DIR so the Azure SDK's
    /// AMQP TLS client trusts it. dotnet dev-certs --trust places the cert in
    /// ~/.aspnet/dev-certs/trust but doesn't update SSL_CERT_DIR automatically.
    ///
    /// Also handles the SSL_CERT_FILE override: when SSL_CERT_FILE is set (e.g. to
    /// /etc/ssl/certs/ca-certificates.crt), OpenSSL ignores SSL_CERT_DIR entirely.
    /// We unset SSL_CERT_FILE so SSL_CERT_DIR takes effect, allowing the dev cert
    /// trust directory to be included in the search path.
    /// </summary>
    public static void EnsureDevCertTrusted()
    {
        var trustDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".aspnet", "dev-certs", "trust");

        if (!Directory.Exists(trustDir))
            return;

        // SSL_CERT_FILE overrides SSL_CERT_DIR in OpenSSL. If it's set, unset it
        // so our SSL_CERT_DIR additions take effect. The system CA bundle is already
        // included via the /usr/lib/ssl/certs directory in SSL_CERT_DIR.
        //
        // We must use native setenv/unsetenv (P/Invoke) because
        // Environment.SetEnvironmentVariable only updates the managed view.
        // OpenSSL reads environment variables via native getenv(3), which is
        // not affected by the managed API.
        var sslCertFile = Environment.GetEnvironmentVariable("SSL_CERT_FILE");
        if (!string.IsNullOrEmpty(sslCertFile))
        {
            Environment.SetEnvironmentVariable("SSL_CERT_FILE", null);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
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
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                setenv("SSL_CERT_DIR", newValue, 1);
        }
    }
}
