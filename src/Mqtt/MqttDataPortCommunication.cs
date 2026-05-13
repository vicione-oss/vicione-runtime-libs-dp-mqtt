using System;
using ViciOne.ManagedEngine.Communication;

namespace ViciOne.Suite.DataPort;

[Communication("mqttdataport")]
public sealed record class MqttDataPortCommunication : DataPortCommunication
{
    public string? ClientId { get; set; }
    public byte Protocol { get; set; }
    public Uri? Url { get; set; }
    public string? Host { get; set; }
    public ushort? Port { get; set; }
    public byte QualityOfService { get; set; }
    public byte ProtocolVersion { get; set; }
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? CertificateFile { get; set; }
    public string? CertificateFilePassword { get; set; }
    public string? CertificatePrivateKeyFile { get; set; }
    public byte? SslProtocol { get; set; }
    public bool? DisableCertificateValidation { get; set; }
    public bool? CleanSession { get; set; }
    public uint? SessionExpiryInterval { get; set; }
    public string? WillTopic { get; set; }
    public string? WillMessage { get; set; }
    public bool? WillRetain { get; set; }
    public bool Pooling { get; set; }
    public int? MaxPendingMessages { get; set; }
    public byte? DefaultSerializer { get; set; }
    public ushort? BrokerReceiveMaximum { get; set; }
}
