using AwesomeAssertions;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttDataPortCommunication_ToString
{
    [Fact]
    public void Redacts_the_secrets()
    {
        MqttDataPortCommunication communication = new()
        {
            Username = "user",
            Password = "s3cret",
            CertificateFile = "-----BEGIN CERTIFICATE-----",
            CertificateFilePassword = "s3cret",
            CertificatePrivateKeyFile = "-----BEGIN PRIVATE KEY-----",
        };

        var text = communication.ToString();

        text.Should().NotContain("s3cret");
        text.Should().NotContain("BEGIN");
        text.Should().Contain("Username = user");
        text.Should().Contain("Password = ***");
        text.Should().Contain("CertificateFile = ***");
        text.Should().Contain("CertificateFilePassword = ***");
        text.Should().Contain("CertificatePrivateKeyFile = ***");
    }

    [Fact]
    public void Keeps_an_unconfigured_secret_empty()
    {
        MqttDataPortCommunication communication = new();

        var text = communication.ToString();

        text.Should().Contain("Password = ,");
        text.Should().NotContain("***");
    }
}
