using System;
using System.Net.Mime;
using Microsoft.Extensions.Logging;
using MQTTnet.Extensions;

namespace ViciOne.Suite.DataPort;

internal static class MqttSetup
{
    internal static IVirtualMqttClient CreateClient(this MqttDataPortCommunication communication, ILoggerFactory loggerFactory)
    {
        var communicationInfo = communication.ToCommunicationInfo();

        if (communication.Pooling)
            return MqttOptimizer.Instance.Register(communicationInfo, loggerFactory);
        return MqttClient.Create(communicationInfo, loggerFactory);
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
            CertificateAuthorityFile = properties.DisableCertificateValidation is true ? null : properties.CertificateAuthorityFile,
            DisableCertificateValidation = properties.DisableCertificateValidation,
            WillTopic = properties.WillTopic,
            WillMessage = properties.WillMessage,
            WillContentType = MediaTypeNames.Text.Plain,
            WillQualityOfService = properties.QualityOfService,
            WillRetain = properties.WillRetain,
            MaxPendingMessages = properties.MaxPendingMessages,
            BrokerReceiveMaximum = properties.BrokerReceiveMaximum,
        };
        switch (properties.Protocol)
        {
            case MqttProtocol.Tcp:
                communicationInfo.TcpHost = properties.Host;
                communicationInfo.TcpPort = properties.Port;
                communicationInfo.SslProtocol = properties.SslProtocol;
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
