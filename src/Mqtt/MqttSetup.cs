using System;
using System.Net.Mime;
using System.Security.Authentication;
using Microsoft.Extensions.Logging;
using MQTTnet.Extensions;
using MQTTnet.Formatter;

namespace ViciOne.Suite.DataPort;

internal static class MqttSetup
{
    private const int PlaintextPort = 1883;
    private const int TlsPort = 8883;

    internal static IVirtualMqttClient CreateClient(this MqttDataPortCommunication communication, ILoggerFactory loggerFactory)
    {
        communication.WarnAboutAPortThatContradictsTheTlsMode(loggerFactory.CreateLogger(typeof(MqttSetup)));
        var communicationInfo = communication.ToCommunicationInfo();

        if (communication.Pooling)
            return MqttOptimizer.Instance.Register(communicationInfo, loggerFactory);
        return MqttClient.Create(communicationInfo, loggerFactory);
    }

    /// <summary>
    /// A broker configured before TLS became the default keeps its plain MQTT port while it now asks
    /// for TLS, and the connection then fails with nothing but a handshake error.
    /// </summary>
    internal static void WarnAboutAPortThatContradictsTheTlsMode(this MqttDataPortCommunication communication, ILogger logger)
    {
        MqttDataPortProperties properties = new(communication);
        if (properties.Protocol != MqttProtocol.Tcp)
            return;

        var usesTls = properties.TlsMode != SslProtocols.None;
        if (usesTls && properties.Port == PlaintextPort)
            logger.LogTlsOnPlaintextPort(PlaintextPort);
        else if (!usesTls && properties.Port == TlsPort)
            logger.LogPlaintextOnTlsPort(TlsPort);
    }

    internal static CommunicationInfo ToCommunicationInfo(this MqttDataPortCommunication communication, Action<CommunicationInfo>? configure = null)
    {
        MqttDataPortProperties properties = new(communication);
        CommunicationInfo communicationInfo = new()
        {
            ProtocolVersion = properties.ProtocolVersion,
            ClientId = properties.ClientId,
            CleanSession = properties.CleanSession,
            SessionExpiryInterval = properties.SessionExpiryInterval,
            Username = properties.Username,
            Password = properties.Password,
            CertificateFile = properties.CertificateFile,
            CertificateFilePassword = properties.CertificateFilePassword,
            CertificatePrivateKeyFile = properties.CertificatePrivateKeyFile,
            CertificateAuthorityFile = properties.ValidateCertificateChain is false ? null : properties.CertificateAuthorityFile,
            DisableCertificateValidation = properties.ValidateCertificateChain is false,
            MaxPendingMessages = properties.MaxPendingMessages,
            BrokerReceiveMaximum = properties.BrokerReceiveMaximum,
        };
        if (properties.ProtocolVersion == MqttProtocolVersion.V500)
            communicationInfo.ClientReceiveMaximum = properties.ClientReceiveMaximum;
        if (properties.LastWillEnabled == true)
        {
            communicationInfo.WillTopic = properties.WillTopic;
            communicationInfo.WillMessage = properties.WillMessage;
            communicationInfo.WillContentType = MediaTypeNames.Text.Plain;
            communicationInfo.WillRetain = properties.WillRetain;
            communicationInfo.WillQualityOfService = properties.WillQualityOfService;
        }
        switch (properties.Protocol)
        {
            case MqttProtocol.Tcp:
                communicationInfo.TcpHost = properties.Host;
                communicationInfo.TcpPort = properties.Port;
                communicationInfo.SslProtocol = properties.TlsMode;
                break;
            case MqttProtocol.WebSocket:
                communicationInfo.WebSocketUri = properties.Url;
                break;
            default: throw new NotSupportedException($"The MQTT communication protocol '{properties.Protocol}' is not supported.");
        }
        configure?.Invoke(communicationInfo);
        return communicationInfo;
    }
}
