using System;
using System.Net.Mime;
using System.Security.Authentication;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using MQTTnet.Formatter;
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
            LastWillEnabled = true,
            WillTopic = "app/life",
            WillMessage = "dead",
            WillRetain = true,
            WillQualityOfService = MqttQualityOfServiceLevel.ExactlyOnce,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.WillTopic.Should().Be("app/life");
        communicationInfo.WillMessage.Should().Be("dead");
        communicationInfo.WillContentType.Should().Be(MediaTypeNames.Text.Plain);
        communicationInfo.WillQualityOfService.Should().Be(MqttQualityOfServiceLevel.ExactlyOnce);
        communicationInfo.WillRetain.Should().BeTrue();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(null)]
    public void Omits_last_will_when_disabled(bool? lastWillEnabled)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            LastWillEnabled = lastWillEnabled,
            WillTopic = "app/life",
            WillMessage = "dead",
            WillRetain = true,
            WillQualityOfService = MqttQualityOfServiceLevel.ExactlyOnce,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.WillTopic.Should().BeNull();
        communicationInfo.WillMessage.Should().BeNull();
        communicationInfo.WillContentType.Should().BeNull();
        communicationInfo.WillRetain.Should().BeNull();
        communicationInfo.WillQualityOfService.Should().Be(MqttQualityOfServiceLevel.AtMostOnce);
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
    public void Does_not_reuse_the_data_quality_of_service_for_the_will()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            LastWillEnabled = true,
            QualityOfService = MqttQualityOfServiceLevel.ExactlyOnce,
            WillQualityOfService = MqttQualityOfServiceLevel.AtLeastOnce,
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
            ValidateCertificateChain = false,
            CertificateAuthorityFile = TestCertificates.CertificateAuthorityPem,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.CertificateAuthorityFile.Should().BeNull();
        communicationInfo.DisableCertificateValidation.Should().BeTrue();
    }

#pragma warning disable CA5398 // The TLS versions the ruleset offers
    [Fact]
    public void Configures_ssl_protocol_for_a_tcp_server()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = "localhost",
            TlsMode = SslProtocols.Tls13,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.SslProtocol.Should().Be(SslProtocols.Tls13);
    }

    [Fact]
    public void Drops_the_ssl_protocol_for_a_web_socket_server()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.WebSocket,
            Url = new Uri("ws://localhost:8000/exchange"),
            TlsMode = SslProtocols.Tls13,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.SslProtocol.Should().BeNull();
    }
#pragma warning restore CA5398 // The TLS versions the ruleset offers

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

    [Theory]
    [InlineData((ushort)10, (ushort)10)]
    [InlineData(null, null)]
    public void Configures_client_receive_maximum(ushort? receiveMaximum, ushort? configuredReceiveMaximum)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            ProtocolVersion = MqttProtocolVersion.V500,
            ClientReceiveMaximum = receiveMaximum,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.ClientReceiveMaximum.Should().Be(configuredReceiveMaximum);
    }

    [Fact]
    public void Omits_client_receive_maximum_for_protocol_version_3_1_1()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            ProtocolVersion = MqttProtocolVersion.V311,
            ClientReceiveMaximum = 65535,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.ClientReceiveMaximum.Should().BeNull();
    }

    [Fact]
    public void Passes_the_certificate_of_a_web_socket_connection_whatever_the_TLS_mode()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.WebSocket,
            Url = new Uri("wss://localhost:8000/exchange"),
            TlsMode = SslProtocols.None,
            CertificateFile = "broker.pfx",
            CertificateFilePassword = "secret",
            CertificatePrivateKeyFile = "broker.key",
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.CertificateFile.Should().Be("broker.pfx");
        communicationInfo.CertificateFilePassword.Should().Be("secret");
        communicationInfo.CertificatePrivateKeyFile.Should().Be("broker.key");
        communicationInfo.SslProtocol.Should().BeNull();
    }

#pragma warning disable CA5398 // Hartcodierte SslProtocols-Werte vermeiden
    [Fact]
    public void Requires_TLS_when_the_mode_is_unset()
    {
        MqttDataPortCommunication communication = new()
        {
            Host = "localhost",
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.SslProtocol.Should().Be(SslProtocols.Tls12 | SslProtocols.Tls13);
    }

    [Fact]
    public void Configures_both_TLS_versions_for_the_automatic_mode()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            TlsMode = SslProtocols.Tls12 | SslProtocols.Tls13,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.SslProtocol.Should().Be(SslProtocols.Tls12 | SslProtocols.Tls13);
    }
#pragma warning restore CA5398 // Hartcodierte SslProtocols-Werte vermeiden

    [Fact]
    public void Configures_no_TLS_for_the_plaintext_mode()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            TlsMode = SslProtocols.None,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.SslProtocol.Should().Be(SslProtocols.None);
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void Configures_certificate_validation(bool? validateCertificateChain, bool disableCertificateValidation)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            ValidateCertificateChain = validateCertificateChain,
        };

        var communicationInfo = communication.ToCommunicationInfo();

        communicationInfo.DisableCertificateValidation.Should().Be(disableCertificateValidation);
    }
}

public class MqttSetup_WarnAboutAPortThatContradictsTheTlsMode
{
#pragma warning disable CA5398 // The TLS versions the ruleset offers
    [Theory]
    [InlineData(SslProtocols.Tls12 | SslProtocols.Tls13, 1883, "*TLS is configured*1883*")]
    [InlineData(SslProtocols.Tls13, 1883, "*TLS is configured*1883*")]
    [InlineData(SslProtocols.None, 8883, "*No TLS*8883*")]
    public void Warns_about_a_port_that_contradicts_the_TLS_mode(SslProtocols tlsMode, int port, string message)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = "localhost",
            Port = port,
            TlsMode = tlsMode,
        };
        FakeLogger logger = new();

        communication.WarnAboutAPortThatContradictsTheTlsMode(logger);

        logger.Collector.Count.Should().Be(1);
        logger.LatestRecord.Level.Should().Be(LogLevel.Warning);
        logger.LatestRecord.Message.Should().MatchEquivalentOf(message);
    }

    [Theory]
    [InlineData(SslProtocols.Tls12 | SslProtocols.Tls13, 8883)]
    [InlineData(SslProtocols.None, 1883)]
    [InlineData(SslProtocols.Tls12 | SslProtocols.Tls13, 9000)]
    public void Stays_silent_for_a_port_that_fits_the_TLS_mode(SslProtocols tlsMode, int port)
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = "localhost",
            Port = port,
            TlsMode = tlsMode,
        };
        FakeLogger logger = new();

        communication.WarnAboutAPortThatContradictsTheTlsMode(logger);

        logger.Collector.Count.Should().Be(0);
    }
#pragma warning restore CA5398 // The TLS versions the ruleset offers

    [Fact]
    public void Stays_silent_for_a_web_socket_connection()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.WebSocket,
            Url = new Uri("ws://localhost:8883/mqtt"),
        };
        FakeLogger logger = new();

        communication.WarnAboutAPortThatContradictsTheTlsMode(logger);

        logger.Collector.Count.Should().Be(0);
    }
}
