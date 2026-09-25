using System;
using System.Security.Authentication;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace ViciOne.Suite.DataPort;

internal sealed class MqttDataPortProperties(MqttDataPortCommunication communication)
{
    internal string? ClientId
    {
        get => communication.ClientId;
        set => communication.ClientId = value;
    }

    internal MqttProtocol Protocol
    {
        get => (MqttProtocol)communication.Protocol;
        set => communication.Protocol = (byte)value;
    }

    internal Uri Url
    {
        get => Protocol != MqttProtocol.WebSocket
                ? throw new InvalidOperationException($"The property '{nameof(communication.Url)}' is only supported by {nameof(Protocol)} '{MqttProtocol.WebSocket}'.")
                : communication.Url
                ?? throw new InvalidOperationException($"The property '{nameof(communication.Url)}' has no value.");
        set => communication.Url = value;
    }

    internal string Host
    {
        get => Protocol != MqttProtocol.Tcp
                ? throw new InvalidOperationException($"The property '{nameof(communication.Host)}' is only supported by {nameof(Protocol)} '{MqttProtocol.Tcp}'.")
                : communication.Host
                ?? throw new InvalidOperationException($"The property '{nameof(communication.Host)}' has no value.");
        set => communication.Host = value;
    }

    internal int? Port
    {
        get => Protocol != MqttProtocol.Tcp
                ? throw new InvalidOperationException($"The property '{nameof(communication.Port)}' is only supported by {nameof(Protocol)} '{MqttProtocol.Tcp}'.")
                : communication.Port;
        set => communication.Port = (ushort?)value;
    }

    internal MqttQualityOfServiceLevel QualityOfService
    {
        get => (MqttQualityOfServiceLevel)communication.QualityOfService;
        set => communication.QualityOfService = (byte)value;
    }

    internal MqttProtocolVersion ProtocolVersion
    {
        get => communication.ProtocolVersion switch
        {
            0 => MqttProtocolVersion.V311,
            1 => MqttProtocolVersion.V500,
            { } x => throw new NotSupportedException($"The {nameof(communication.ProtocolVersion)} '{x}' is not supported."),
        };
        set => communication.ProtocolVersion = value switch
        {
            MqttProtocolVersion.V311 => 0,
            MqttProtocolVersion.V500 => 1,
            { } x => throw new NotSupportedException($"The {nameof(communication.ProtocolVersion)} '{x}' is not supported."),
        };
    }

    internal string? Username
    {
        get => communication.Username;
        set => communication.Username = value;
    }

    internal string? Password
    {
        get => communication.Password;
        set => communication.Password = value;
    }

    internal string? CertificateFile
    {
        get => communication.CertificateFile;
        set => communication.CertificateFile = value;
    }

    internal string? CertificateFilePassword
    {
        get => communication.CertificateFilePassword;
        set => communication.CertificateFilePassword = value;
    }

    internal string? CertificatePrivateKeyFile
    {
        get => communication.CertificatePrivateKeyFile;
        set => communication.CertificatePrivateKeyFile = value;
    }

    internal string? CertificateAuthorityFile
    {
        get => communication.CertificateAuthorityFile;
        set => communication.CertificateAuthorityFile = value;
    }

#pragma warning disable CA5398 // The ruleset offers exactly these TLS versions to choose from
    internal SslProtocols? SslProtocol
    {
        get => communication.SslProtocol switch
        {
            null => null,
            0 => SslProtocols.None,
            1 => SslProtocols.Tls12,
            2 => SslProtocols.Tls13,
            { } p => throw new NotSupportedException($"The {nameof(communication.SslProtocol)} '{p}' is not supported."),
        };
        set => communication.SslProtocol = value switch
        {
            null => null,
            SslProtocols.None => 0,
            SslProtocols.Tls12 => 1,
            SslProtocols.Tls13 => 2,
            { } p => throw new NotSupportedException($"The {nameof(communication.SslProtocol)} '{p}' is not supported."),
        };
#pragma warning restore CA5398 // The ruleset offers exactly these TLS versions to choose from
    }

    internal bool? DisableCertificateValidation
    {
        get => communication.DisableCertificateValidation;
        set => communication.DisableCertificateValidation = value;
    }

    internal bool? CleanSession
    {
        get => communication.CleanSession;
        set => communication.CleanSession = value;
    }

    internal uint? SessionExpiryInterval
    {
        get => communication.SessionExpiryInterval;
        set => communication.SessionExpiryInterval = value;
    }
    internal string? WillTopic
    {
        get => communication.WillTopic;
        set => communication.WillTopic = value;
    }

    internal string? WillMessage
    {
        get => communication.WillMessage;
        set => communication.WillMessage = value;
    }

    internal bool? WillRetain
    {
        get => communication.WillRetain;
        set => communication.WillRetain = value;
    }

    internal int? MaxPendingMessages
    {
        get => communication.MaxPendingMessages;
        set => communication.MaxPendingMessages = value;
    }

    public ushort? BrokerReceiveMaximum
    {
        get => communication.BrokerReceiveMaximum;
        set => communication.BrokerReceiveMaximum = value;
    }

    internal Serializer DefaultSerializer
    {
        get => communication.DefaultSerializer switch
        {
            null => Serializer.Json,
            1 => Serializer.Json,
            { } p => throw new NotSupportedException($"The {nameof(communication.DefaultSerializer)} '{p}' is not supported."),
        };
        set => communication.DefaultSerializer = value switch
        {
            Serializer.Json => 1,
            { } p => throw new NotSupportedException($"The {nameof(communication.DefaultSerializer)} '{p}' is not supported."),
        };
    }
}
