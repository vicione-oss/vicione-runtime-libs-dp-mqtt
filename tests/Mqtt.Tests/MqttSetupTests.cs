using System;
using System.Net.Mime;
using AwesomeAssertions;
using MQTTnet.Protocol;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttSetup_ToCommunicationInfo
{
    [Fact]
    public void Configures_client_id()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            ClientId = "Test",
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.ClientId.Should().Be("Test");
    }

    [Fact]
    public void Configures_last_will()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            QualityOfService = MqttQualityOfServiceLevel.ExactlyOnce,
            WillTopic = "app/life",
            WillMessage = "dead",
            WillRetain = true,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.WillTopic.Should().Be("app/life");
        communicationInfo.WillMessage.Should().Be("dead");
        communicationInfo.WillContentType.Should().Be(MediaTypeNames.Text.Plain);
        communicationInfo.WillQualityOfService.Should().Be(MqttQualityOfServiceLevel.ExactlyOnce);
        communicationInfo.WillRetain.Should().BeTrue();
    }

    [Fact]
    public void Configures_tcp_server()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = "localhost",
            Port = 1884,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.TcpHost.Should().Be("localhost");
        communicationInfo.TcpPort.Should().Be(1884);
        communicationInfo.WebSocketUri.Should().BeNull();
    }

    [Fact]
    public void Configures_web_socket_server()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.WebSocket,
            Url = new Uri("wss://localhost:8000/exchange"),
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.TcpHost.Should().BeNull();
        communicationInfo.TcpPort.Should().BeNull();
        communicationInfo.WebSocketUri.Should().Be(new Uri("wss://localhost:8000/exchange"));
    }

    [Fact]
    public void Configures_quality_of_server()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            QualityOfService = MqttQualityOfServiceLevel.AtLeastOnce,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.WillQualityOfService.Should().Be(MqttQualityOfServiceLevel.AtLeastOnce);
    }

    [Fact]
    public void Throws_NotSupported_for_unknown_server_communication()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = (MqttProtocol)2,
        };

        var call = FluentActions.Invoking(() => communication.ToCommunicationInfo());

        call.Should().Throw<NotSupportedException>().WithMessage("*communication*protocol*'2'*not*supported*");
    }

    [Theory]
    [InlineData("", "abc")]
    [InlineData("user", "abc")]
    [InlineData("user", "")]
    public void Configures_credentials(string username, string password)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            Username = username,
            Password = password,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.Username.Should().Be(username);
        communicationInfo.Password.Should().Be(password);
    }

    [Fact]
    public void Configures_certificate_authority()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            CertificateAuthorityFile = "/etc/mqtt/ca.pem",
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.CertificateAuthorityFile.Should().Be("/etc/mqtt/ca.pem");
    }

    [Fact]
    public void Drops_the_certificate_authority_while_the_certificate_validation_is_disabled()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            DisableCertificateValidation = true,
            CertificateAuthorityFile = TestCertificates.CertificateAuthorityPem,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.CertificateAuthorityFile.Should().BeNull();
        communicationInfo.DisableCertificateValidation.Should().BeTrue();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(null, null)]
    public void Configures_clean_session(bool? cleanSession, bool? configuredCleanSession)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            CleanSession = cleanSession,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.CleanSession.Should().Be(configuredCleanSession);
    }

    [Theory]
    [InlineData(10u, 10u)]
    [InlineData(0u, 0u)]
    [InlineData(null, null)]
    public void Configures_session_expiry_interval(uint? interval, uint? configuredInterval)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            SessionExpiryInterval = interval,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.SessionExpiryInterval.Should().Be(configuredInterval);
    }
}
