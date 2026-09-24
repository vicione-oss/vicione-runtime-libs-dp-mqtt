using AwesomeAssertions;
using MQTTnet.Extensions;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class CommunicationInfoExtensions_BuildClientOptions
{
    [Fact]
    public void Validates_the_broker_against_a_certificate_authority()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            CertificateAuthorityFile = TestCertificates.CertificateAuthorityPem,
        };

        var options = communication.ToCommunicationInfo().BuildClientOptions();

        var tlsOptions = options.ClientOptions!.ChannelOptions.TlsOptions;
        tlsOptions.UseTls.Should().BeTrue();
        tlsOptions.TrustChain.Should().ContainSingle();
    }
}
