using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AlmostServiceBus.Core.Hosting;

namespace AlmostServiceBus.Tests.Hosting;

public class EmulatorCertificateTests : IDisposable
{
    private readonly string _certDir = Path.Combine(Path.GetTempPath(), "asb-cert-tests-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_certDir))
            Directory.Delete(_certDir, recursive: true);
    }

    [Fact]
    public void GetOrCreate_GeneratesLeafWithServerAuthAndRequestedSans()
    {
        var bundle = EmulatorCertificate.GetOrCreate(_certDir, ["example.test", "10.0.0.5"]);

        Assert.True(bundle.Generated);
        using var leaf = bundle.ServerCertificate;

        var eku = leaf.Extensions.OfType<X509EnhancedKeyUsageExtension>().Single();
        Assert.Contains(eku.EnhancedKeyUsages.Cast<System.Security.Cryptography.Oid>(), o => o.Value == "1.3.6.1.5.5.7.3.1");

        var san = leaf.Extensions.Single(e => e.Oid?.Value == "2.5.29.17").Format(false);
        Assert.Contains("localhost", san);
        Assert.Contains("127.0.0.1", san);
        Assert.Contains("example.test", san);
        Assert.Contains("10.0.0.5", san);

        Assert.True(leaf.HasPrivateKey);
    }

    [Fact]
    public void GetOrCreate_LeafChainsToEmittedCa()
    {
        var bundle = EmulatorCertificate.GetOrCreate(_certDir, ["localhost"]);
        using var leaf = bundle.ServerCertificate;
        using var ca = X509CertificateLoader.LoadCertificateFromFile(bundle.CaCertPath);

        using var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);

        Assert.True(chain.Build(leaf), string.Join(", ", chain.ChainStatus.Select(s => s.StatusInformation)));
    }

    [Fact]
    public void GetOrCreate_TrustStoreContainsCaCertificate()
    {
        var bundle = EmulatorCertificate.GetOrCreate(_certDir, ["localhost"]);
        var collection = X509CertificateLoader.LoadPkcs12CollectionFromFile(
            bundle.TrustStorePath, bundle.TrustStorePassword);

        var subjects = string.Join(" | ", collection.Cast<X509Certificate2>().Select(c => c.Subject));
        Assert.True(
            collection.Cast<X509Certificate2>().Any(c => c.Subject.Contains("AlmostServiceBus Emulator Dev CA")),
            $"count={collection.Count} subjects=[{subjects}]");
    }

    [Fact]
    public void GetOrCreate_ReusesExistingMaterialOnSecondCall()
    {
        var first = EmulatorCertificate.GetOrCreate(_certDir, ["localhost"]);
        var firstThumbprint = first.ServerCertificate.Thumbprint;
        first.ServerCertificate.Dispose();

        var second = EmulatorCertificate.GetOrCreate(_certDir, ["localhost"]);
        using var reused = second.ServerCertificate;

        Assert.False(second.Generated);
        Assert.Equal(firstThumbprint, reused.Thumbprint);
    }

    [Fact]
    public void FromUserCertificate_LoadsPfxFileAndEmitsTrustMaterial()
    {
        using var supplied = CreateSelfSignedServerCertificate();
        var pfxPath = Path.Combine(_certDir, "mine.pfx");
        Directory.CreateDirectory(_certDir);
        File.WriteAllBytes(pfxPath, supplied.Export(X509ContentType.Pfx, "secret"));

        var bundle = EmulatorCertificate.FromUserCertificate(_certDir, certPath: pfxPath, certPassword: "secret");

        using var server = bundle.ServerCertificate;
        Assert.True(bundle.UserSupplied);
        Assert.False(bundle.Generated);
        Assert.True(server.HasPrivateKey);
        Assert.Equal(supplied.Thumbprint, server.Thumbprint);

        AssertTrustMaterialContains(bundle, supplied.Thumbprint);
    }

    [Fact]
    public void FromUserCertificate_LoadsBase64Pfx()
    {
        using var supplied = CreateSelfSignedServerCertificate();
        var base64 = Convert.ToBase64String(supplied.Export(X509ContentType.Pfx, "secret"));

        var bundle = EmulatorCertificate.FromUserCertificate(_certDir, certPassword: "secret", certBase64: base64);

        using var server = bundle.ServerCertificate;
        Assert.True(bundle.UserSupplied);
        Assert.True(server.HasPrivateKey);
        Assert.Equal(supplied.Thumbprint, server.Thumbprint);
        AssertTrustMaterialContains(bundle, supplied.Thumbprint);
    }

    [Fact]
    public void FromUserCertificate_LoadsPemCertAndKeyFiles()
    {
        using var supplied = CreateSelfSignedServerCertificate();
        Directory.CreateDirectory(_certDir);
        var certPem = Path.Combine(_certDir, "mine.crt");
        var keyPem = Path.Combine(_certDir, "mine.key");
        File.WriteAllText(certPem, supplied.ExportCertificatePem());
        File.WriteAllText(keyPem, supplied.GetRSAPrivateKey()!.ExportRSAPrivateKeyPem());

        var bundle = EmulatorCertificate.FromUserCertificate(_certDir, certPath: certPem, keyPath: keyPem);

        using var server = bundle.ServerCertificate;
        Assert.True(bundle.UserSupplied);
        Assert.True(server.HasPrivateKey);
        Assert.Equal(supplied.Thumbprint, server.Thumbprint);
        AssertTrustMaterialContains(bundle, supplied.Thumbprint);
    }

    [Fact]
    public void FromUserCertificate_ThrowsWhenBase64Invalid()
    {
        Assert.Throws<InvalidOperationException>(
            () => EmulatorCertificate.FromUserCertificate(_certDir, certBase64: "not-base64!!!"));
    }

    private void AssertTrustMaterialContains(EmulatorCertificate.CertificateBundle bundle, string thumbprint)
    {
        using var caFromPem = X509CertificateLoader.LoadCertificateFromFile(bundle.CaCertPath);
        Assert.Equal(thumbprint, caFromPem.Thumbprint);

        var trustStore = X509CertificateLoader.LoadPkcs12CollectionFromFile(bundle.TrustStorePath, bundle.TrustStorePassword);
        Assert.Contains(trustStore.Cast<X509Certificate2>(), c => c.Thumbprint == thumbprint);
    }

    private static X509Certificate2 CreateSelfSignedServerCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=my-own-emulator-cert", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, critical: false));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        request.CertificateExtensions.Add(san.Build());
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
    }
}