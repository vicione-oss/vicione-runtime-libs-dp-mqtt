using System;
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

    [Theory]
    [InlineData("ws://localhost:8000/exchange")]
    [InlineData("wss://localhost:8000/exchange")]
    public void Accepts_a_web_socket_url(string configured)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.WebSocket,
            Url = new Uri(configured),
        };

        var call = FluentActions.Invoking(() => communication.ToCommunicationInfo().BuildClientOptions());

        call.Should().NotThrow();
    }

    [Theory]
    [InlineData("localhost:8000/exchange")]
    [InlineData("http://localhost:8000/exchange")]
    [InlineData("mqtt://localhost:8000")]
    public void Rejects_a_url_without_a_web_socket_scheme(string configured)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.WebSocket,
            Url = new Uri(configured),
        };

        var call = FluentActions.Invoking(() => communication.ToCommunicationInfo().BuildClientOptions());

        call.Should().Throw<NotSupportedException>().WithMessage("*ws*wss*");
    }
}
