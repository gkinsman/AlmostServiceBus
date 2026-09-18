using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using AlmostServiceBus.Aspire.Hosting;

namespace AlmostServiceBus.Aspire.Tests;

public class WithAdminTlsTests
{
    [Fact]
    public async Task WithAdminTls_registers_https_admin_endpoint_on_default_port()
    {
        var (certPath, password) = CreateTestPfx();

        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder
            .AddServiceBusEmulator("servicebus")
            .WithAdminTls(certPath: certPath, certPassword: password)
            .Resource;

        var endpoint = resource.Annotations.OfType<EndpointAnnotation>()
            .SingleOrDefault(e => e.Name == "admintls");

        Assert.NotNull(endpoint);
        Assert.Equal("https", endpoint!.UriScheme);
        Assert.Equal(5301, endpoint.Port);
    }

    [Fact]
    public async Task WithAdminTls_appends_enabled_port_and_supplied_cert_args()
    {
        var (certPath, password) = CreateTestPfx();

        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder
            .AddServiceBusEmulator("servicebus")
            .WithAdminTls(certPath: certPath, certPassword: password)
            .Resource;

        var args = await ResolveArgsAsync(resource);

        AssertArgPair(args, "--AdminTlsEnabled", "true");
        AssertArgPair(args, "--AdminTlsPort", "5301");
        AssertArgPair(args, "--AdminTlsCertPath", certPath);
        AssertArgPair(args, "--AdminTlsCertPassword", password);
    }

    [Fact]
    public async Task WithAdminTls_honours_custom_port()
    {
        var (certPath, password) = CreateTestPfx();

        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder
            .AddServiceBusEmulator("servicebus")
            .WithAdminTls(port: 443, certPath: certPath, certPassword: password)
            .Resource;

        var endpoint = resource.Annotations.OfType<EndpointAnnotation>()
            .Single(e => e.Name == "admintls");
        Assert.Equal(443, endpoint.Port);

        var args = await ResolveArgsAsync(resource);
        AssertArgPair(args, "--AdminTlsPort", "443");
    }

    [Fact]
    public async Task WithAdminTls_with_byo_cert_does_not_emit_password_when_none_supplied()
    {
        // A PEM cert + key pair is served without a password.
        var (certPath, _) = CreateTestPfx();

        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder
            .AddServiceBusEmulator("servicebus")
            .WithAdminTls(certPath: certPath)
            .Resource;

        var args = await ResolveArgsAsync(resource);

        AssertArgPair(args, "--AdminTlsCertPath", certPath);
        Assert.DoesNotContain("--AdminTlsCertPassword", args.OfType<string>());
    }

    [Fact]
    public async Task Without_WithAdminTls_no_tls_endpoint_or_args()
    {
        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder
            .AddServiceBusEmulator("servicebus")
            .Resource;

        Assert.DoesNotContain(resource.Annotations.OfType<EndpointAnnotation>(), e => e.Name == "admintls");

        var args = await ResolveArgsAsync(resource);
        Assert.DoesNotContain("--AdminTlsEnabled", args.OfType<string>());
    }

    [Fact]
    public async Task WithAdminTls_default_exports_dev_cert_and_passes_it()
    {
        // The default path exports the ASP.NET Core HTTPS dev cert. Ensure one exists first so the
        // export is deterministic; skip if the SDK cannot provide one in this environment.
        if (!EnsureDevCert())
            return;

        var builder = DistributedApplication.CreateBuilder(Array.Empty<string>());
        var resource = builder
            .AddServiceBusEmulator("servicebus")
            .WithAdminTls()
            .Resource;

        var args = await ResolveArgsAsync(resource);

        AssertArgPair(args, "--AdminTlsEnabled", "true");

        var certPath = ValueAfter(args, "--AdminTlsCertPath");
        Assert.NotNull(certPath);
        Assert.True(File.Exists(certPath), $"exported dev cert should exist at {certPath}");

        var password = ValueAfter(args, "--AdminTlsCertPassword");
        Assert.False(string.IsNullOrEmpty(password));
    }

    private static async Task<List<object>> ResolveArgsAsync(IResource resource)
    {
        var args = new List<object>();
        var context = new CommandLineArgsCallbackContext(args);
        foreach (var annotation in resource.Annotations.OfType<CommandLineArgsCallbackAnnotation>())
            await annotation.Callback(context);
        return args;
    }

    private static void AssertArgPair(List<object> args, string flag, string value)
    {
        var actual = ValueAfter(args, flag);
        Assert.True(actual == value, $"expected '{flag} {value}' but found '{flag} {actual ?? "<missing>"}'");
    }

    private static string? ValueAfter(List<object> args, string flag)
    {
        for (var i = 0; i < args.Count - 1; i++)
        {
            if (args[i] is string s && s == flag)
                return args[i + 1] as string;
        }
        return null;
    }

    private static (string CertPath, string Password) CreateTestPfx()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        const string password = "test-pw";
        var pfx = cert.Export(X509ContentType.Pfx, password);
        var path = Path.Combine(Path.GetTempPath(), $"asb-aspire-test-{Guid.NewGuid():N}.pfx");
        File.WriteAllBytes(path, pfx);
        return (path, password);
    }

    private static bool EnsureDevCert()
    {
        try
        {
            var psi = new System.Diagnostics.ProcessStartInfo("dotnet")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            psi.ArgumentList.Add("dev-certs");
            psi.ArgumentList.Add("https");
            using var proc = System.Diagnostics.Process.Start(psi);
            if (proc is null)
                return false;
            proc.WaitForExit();
            return proc.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
