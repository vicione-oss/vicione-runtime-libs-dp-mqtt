using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace ViciOne.Suite.DataPort;

internal static class TestCertificates
{
    internal static string CertificateAuthorityPem { get; } = CreateCertificateAuthorityPem();

    private static string CreateCertificateAuthorityPem()
    {
        using var key = RSA.Create(2048);
        CertificateRequest request = new("CN=mqtt-ca", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
        using var authority = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        return authority.ExportCertificatePem();
    }
}
