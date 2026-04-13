using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;

namespace AlmostServiceBus.Core.Hosting;

/// <summary>
/// Shared infrastructure utilities used by both the standalone host and the test fixture.
/// </summary>
public static class EmulatorInfrastructure
{
    /// <summary>
    /// Finds an available TCP port on the loopback interface.
    /// </summary>
    public static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
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
        var sslCertFile = Environment.GetEnvironmentVariable("SSL_CERT_FILE");
        if (!string.IsNullOrEmpty(sslCertFile))
        {
            Environment.SetEnvironmentVariable("SSL_CERT_FILE", null);
        }

        var current = Environment.GetEnvironmentVariable("SSL_CERT_DIR") ?? "";
        if (!current.Contains(trustDir))
        {
            var systemCerts = "/usr/lib/ssl/certs";
            var newValue = string.IsNullOrEmpty(current)
                ? $"{trustDir}:{systemCerts}"
                : $"{trustDir}:{current}";
            Environment.SetEnvironmentVariable("SSL_CERT_DIR", newValue);
        }
    }
}
