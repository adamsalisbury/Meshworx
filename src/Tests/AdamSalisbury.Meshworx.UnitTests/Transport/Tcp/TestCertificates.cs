using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

/// <summary>
/// Builds throwaway self-signed certificates for the TLS transport tests, so the suite needs no
/// checked-in key material and nothing that can expire.
/// </summary>
internal static class TestCertificates
{
    /// <summary>
    /// Creates a self-signed certificate valid for "localhost" with an exportable private key.
    /// </summary>
    /// <param name="subjectName">The common name to issue the certificate to.</param>
    internal static X509Certificate2 CreateSelfSigned(string subjectName = "localhost")
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={subjectName}",
            key,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);

        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(certificateAuthority: false, false, 0, critical: true));
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: true));
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension(
                [
                    new Oid("1.3.6.1.5.5.7.3.1"),  // Server authentication.
                    new Oid("1.3.6.1.5.5.7.3.2"),  // Client authentication.
                ],
                critical: false));

        var subjectAlternativeName = new SubjectAlternativeNameBuilder();
        subjectAlternativeName.AddDnsName(subjectName);
        subjectAlternativeName.AddIpAddress(System.Net.IPAddress.Loopback);
        request.CertificateExtensions.Add(subjectAlternativeName.Build());

        using X509Certificate2 certificate = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddHours(1));

        // Round-tripping through PKCS#12 gives the certificate a private key SslStream can use as a
        // server on every platform; the key attached by CreateSelfSigned is ephemeral and is rejected on
        // some of them.
        return X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pkcs12), password: null);
    }

    /// <summary>
    /// Builds a validation callback that accepts exactly the given certificate and nothing else, so the
    /// tests pin rather than blanket-trusting whatever is presented.
    /// </summary>
    /// <param name="expected">The only certificate that should be accepted.</param>
    internal static RemoteCertificateValidationCallback PinnedTo(X509Certificate2 expected)
    {
        string expectedThumbprint = expected.Thumbprint;

        return (_, certificate, _, _) =>
            certificate is X509Certificate2 presented
            && string.Equals(presented.Thumbprint, expectedThumbprint, StringComparison.OrdinalIgnoreCase);
    }
}
