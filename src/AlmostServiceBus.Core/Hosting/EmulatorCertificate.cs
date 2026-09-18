using System.Formats.Asn1;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.Pkcs;
using System.Security.Cryptography.X509Certificates;
namespace AlmostServiceBus.Core.Hosting;

/// <summary>
/// Provides the TLS material for the emulator's HTTPS admin endpoint. The certificate is
/// generated once into <c>certDir</c> and reused on every subsequent start, so a mounted
/// volume gives a stable CA that clients trust a single time. Emits the CA in the formats the
/// non-.NET admin clients need: a PEM anchor (Node/Python/curl) and a PKCS#12 truststore (Java).
/// </summary>
public static class EmulatorCertificate
{
    public const string CaFileName = "emulator-ca.crt";
    public const string ServerPfxFileName = "emulator-admin.pfx";
    public const string TrustStoreFileName = "emulator-truststore.p12";

    // Dev-only material for a local, otherwise-plaintext emulator; "changeit" is the JVM default
    // truststore password so Java users can point at the file without extra flags.
    private const string Password = "changeit";

    public sealed record CertificateBundle(
        X509Certificate2 ServerCertificate,
        string CaCertPath,
        string TrustStorePath,
        string TrustStorePassword,
        bool Generated,
        bool UserSupplied = false);

    /// <summary>
    /// Builds a bundle from a caller-supplied certificate instead of generating one. The
    /// certificate can come from a PKCS#12/PFX file, a PEM certificate (optionally with a separate
    /// PEM key file), or a base64-encoded PFX (handy for passing a cert into a container via an
    /// environment variable). The server certificate is used as-is; its public certificate (plus
    /// any additional certs in the chain) is emitted as the CA PEM and Java truststore so the
    /// non-.NET clients can trust it the same way they trust the generated CA.
    /// </summary>
    public static CertificateBundle FromUserCertificate(
        string certDir,
        string? certPath = null,
        string? keyPath = null,
        string? certPassword = null,
        string? certBase64 = null)
    {
        Directory.CreateDirectory(certDir);

        X509Certificate2 serverCertificate;
        var anchors = new X509Certificate2Collection();

        if (!string.IsNullOrWhiteSpace(certBase64))
        {
            byte[] pfxBytes;
            try { pfxBytes = Convert.FromBase64String(certBase64.Trim()); }
            catch (FormatException ex) { throw new InvalidOperationException("AdminTlsCertBase64 is not valid base64.", ex); }
            LoadPkcs12(pfxBytes, certPassword, out serverCertificate, anchors);
        }
        else if (!string.IsNullOrWhiteSpace(certPath))
        {
            if (!File.Exists(certPath))
                throw new FileNotFoundException($"Admin TLS certificate not found: {certPath}", certPath);

            if (!string.IsNullOrWhiteSpace(keyPath) || IsPemFile(certPath))
                LoadPem(certPath, keyPath, out serverCertificate, anchors);
            else
                LoadPkcs12(File.ReadAllBytes(certPath), certPassword, out serverCertificate, anchors);
        }
        else
        {
            throw new InvalidOperationException("No user certificate provided; expected a path or base64 value.");
        }

        if (!serverCertificate.HasPrivateKey)
            throw new InvalidOperationException("The supplied Admin TLS certificate has no private key; Kestrel cannot serve TLS without one.");

        var caPath = Path.Combine(certDir, CaFileName);
        var trustStorePath = Path.Combine(certDir, TrustStoreFileName);
        File.WriteAllText(caPath, ExportCertificatesPem(anchors));
        File.WriteAllBytes(trustStorePath, BuildJavaTrustStore(anchors));

        return new CertificateBundle(serverCertificate, caPath, trustStorePath, Password, Generated: false, UserSupplied: true);
    }

    private static void LoadPkcs12(byte[] pfxBytes, string? password, out X509Certificate2 server, X509Certificate2Collection anchors)
    {
        var collection = X509CertificateLoader.LoadPkcs12Collection(pfxBytes, password, X509KeyStorageFlags.Exportable);
        server = collection.OfType<X509Certificate2>().FirstOrDefault(c => c.HasPrivateKey)
            ?? throw new InvalidOperationException("The supplied PKCS#12 certificate does not contain a private key.");
        anchors.AddRange(collection);
    }

    private static void LoadPem(string certPath, string? keyPath, out X509Certificate2 server, X509Certificate2Collection anchors)
    {
        using var pemCert = X509Certificate2.CreateFromPemFile(certPath, string.IsNullOrWhiteSpace(keyPath) ? null : keyPath);
        // Round-trip through PKCS#12 so the private key is usable by SslStream/Kestrel on Linux.
        server = X509CertificateLoader.LoadPkcs12(pemCert.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);
        anchors.ImportFromPemFile(certPath);
    }

    private static bool IsPemFile(string path)
    {
        using var reader = new StreamReader(path);
        var head = new char[64];
        var read = reader.Read(head, 0, head.Length);
        return new string(head, 0, read).Contains("-----BEGIN", StringComparison.Ordinal);
    }

    /// <summary>
    /// Loads the persisted certificate from <paramref name="certDir"/> if present, otherwise
    /// generates a CA + server leaf covering <paramref name="hosts"/> (plus loopback) and writes
    /// the CA PEM, server PFX and Java truststore into the directory.
    /// </summary>
    public static CertificateBundle GetOrCreate(string certDir, IEnumerable<string> hosts)
    {
        Directory.CreateDirectory(certDir);

        var caPath = Path.Combine(certDir, CaFileName);
        var pfxPath = Path.Combine(certDir, ServerPfxFileName);
        var trustStorePath = Path.Combine(certDir, TrustStoreFileName);

        if (File.Exists(caPath) && File.Exists(pfxPath) && File.Exists(trustStorePath))
        {
            var existing = X509CertificateLoader.LoadPkcs12FromFile(pfxPath, Password, X509KeyStorageFlags.Exportable);
            return new CertificateBundle(existing, caPath, trustStorePath, Password, Generated: false);
        }

        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var caNotAfter = DateTimeOffset.UtcNow.AddYears(10);
        var leafNotAfter = DateTimeOffset.UtcNow.AddYears(2);

        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=AlmostServiceBus Emulator Dev CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: true, hasPathLengthConstraint: true, pathLengthConstraint: 0, critical: true));
        caRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, critical: true));
        caRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(caRequest.PublicKey, critical: false));
        using var caCert = caRequest.CreateSelfSigned(notBefore, caNotAfter);

        using var leafKey = RSA.Create(2048);
        var leafRequest = new CertificateRequest("CN=AlmostServiceBus Emulator", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        leafRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: false, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));
        leafRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: true));
        leafRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, critical: false));
        leafRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(leafRequest.PublicKey, critical: false));
        // OpenSSL 3.x (used by Python's requests, curl, etc.) rejects a leaf whose Authority Key
        // Identifier does not chain to the CA's Subject Key Identifier, so emit it explicitly.
        leafRequest.CertificateExtensions.Add(
            X509AuthorityKeyIdentifierExtension.CreateFromCertificate(caCert, includeKeyIdentifier: true, includeIssuerAndSerial: false));
        leafRequest.CertificateExtensions.Add(BuildSubjectAlternativeNames(hosts));

        var serial = RandomNumberGenerator.GetBytes(16);
        serial[0] &= 0x7F; // keep the serial positive
        using var leaf = leafRequest.Create(caCert, notBefore, leafNotAfter, serial);
        using var leafWithKey = leaf.CopyWithPrivateKey(leafKey);

        // Round-trip through PKCS#12 so the private key is usable by SslStream/Kestrel on Linux.
        var pfxBytes = leafWithKey.Export(X509ContentType.Pfx, Password);

        File.WriteAllText(caPath, caCert.ExportCertificatePem());
        File.WriteAllBytes(pfxPath, pfxBytes);
        File.WriteAllBytes(trustStorePath, BuildJavaTrustStore(caCert));

        var serverCertificate = X509CertificateLoader.LoadPkcs12(pfxBytes, Password, X509KeyStorageFlags.Exportable);
        return new CertificateBundle(serverCertificate, caPath, trustStorePath, Password, Generated: true);
    }

    private static X509Extension BuildSubjectAlternativeNames(IEnumerable<string> hosts)
    {
        var sanBuilder = new SubjectAlternativeNameBuilder();
        sanBuilder.AddDnsName("localhost");
        sanBuilder.AddIpAddress(IPAddress.Loopback);
        sanBuilder.AddIpAddress(IPAddress.IPv6Loopback);

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "localhost", "127.0.0.1", "::1" };
        foreach (var host in hosts)
        {
            var candidate = host?.Trim();
            if (string.IsNullOrEmpty(candidate) || !seen.Add(candidate))
                continue;

            if (IPAddress.TryParse(candidate, out var ip))
                sanBuilder.AddIpAddress(ip);
            else
                sanBuilder.AddDnsName(candidate);
        }

        return sanBuilder.Build();
    }

    private static byte[] BuildJavaTrustStore(X509Certificate2 caCert)
        => BuildJavaTrustStore(new X509Certificate2Collection(caCert));

    // A truststore is a PKCS#12 holding only public certificates (no private keys). Java's PKCS#12
    // keystore only surfaces a cert as a trusted entry when it carries a friendly-name alias and
    // Oracle's trustedKeyUsage attribute, so a plain collection export (no attributes) shows up as
    // zero entries. Build each bag explicitly with both.
    private static byte[] BuildJavaTrustStore(X509Certificate2Collection anchors)
    {
        var contents = new Pkcs12SafeContents();
        var index = 0;
        foreach (var anchor in anchors.OfType<X509Certificate2>())
        {
            using var caPublic = X509CertificateLoader.LoadCertificate(anchor.Export(X509ContentType.Cert));
            var bag = contents.AddCertificate(caPublic);

            var friendlyName = new AsnWriter(AsnEncodingRules.DER);
            friendlyName.WriteCharacterString(UniversalTagNumber.BMPString, $"almostservicebus-ca-{index++}");
            bag.Attributes.Add(new Pkcs9AttributeObject(FriendlyNameOid, friendlyName.Encode()));

            // Oracle trustedKeyUsage = anyExtendedKeyUsage marks the cert as a trusted anchor.
            var trustedUsage = new AsnWriter(AsnEncodingRules.DER);
            trustedUsage.WriteObjectIdentifier("2.5.29.37.0");
            bag.Attributes.Add(new Pkcs9AttributeObject(TrustedKeyUsageOid, trustedUsage.Encode()));
        }

        var builder = new Pkcs12Builder();
        builder.AddSafeContentsUnencrypted(contents);
        builder.SealWithMac(Password, HashAlgorithmName.SHA256, 100_000);
        return builder.Encode();
    }

    private static string ExportCertificatesPem(X509Certificate2Collection anchors)
        => string.Join('\n', anchors.OfType<X509Certificate2>().Select(c => c.ExportCertificatePem())) + "\n";

    private const string FriendlyNameOid = "1.2.840.113549.1.9.20";
    private const string TrustedKeyUsageOid = "2.16.840.1.113894.746875.1.1";
}
