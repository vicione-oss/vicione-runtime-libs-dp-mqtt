using System.Diagnostics.CodeAnalysis;

namespace ViciOne.Suite.DataPort;

internal static class MqttDataPortPropertyExtensions
{
    internal static bool HasCertificateFile(this MqttDataPortProperties properties, [NotNullWhen(true)] out string? certificateFile)
    {
        certificateFile = properties.CertificateFile;
        return !string.IsNullOrWhiteSpace(properties.CertificateFile);
    }

    internal static bool HasCertificateFilePassword(this MqttDataPortProperties properties, [NotNullWhen(true)] out string? certificateFilePassword)
    {
        certificateFilePassword = properties.CertificateFilePassword;
        return !string.IsNullOrEmpty(properties.CertificateFilePassword);
    }

    internal static bool HasCertificatePrivateKeyFile(this MqttDataPortProperties properties, [NotNullWhen(true)] out string? certificateFilePrivateKey)
    {
        certificateFilePrivateKey = properties.CertificatePrivateKeyFile;
        return !string.IsNullOrWhiteSpace(properties.CertificatePrivateKeyFile);
    }
}
