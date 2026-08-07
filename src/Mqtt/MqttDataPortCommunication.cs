using System;
using System.Text;
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

    /// <summary>
    /// The generated text representation of a record prints every property, which writes the
    /// credentials and the private key of the client certificate into every log sink they reach.
    /// </summary>
    protected override bool PrintMembers(StringBuilder builder)
    {
        if (base.PrintMembers(builder))
            builder.Append(", ");

        builder.Append("ClientId = ").Append(ClientId);
        builder.Append(", Protocol = ").Append(Protocol);
        builder.Append(", Url = ").Append(Url);
        builder.Append(", Host = ").Append(Host);
        builder.Append(", Port = ").Append(Port);
        builder.Append(", QualityOfService = ").Append(QualityOfService);
        builder.Append(", ProtocolVersion = ").Append(ProtocolVersion);
        builder.Append(", Username = ").Append(Username);
        builder.Append(", Password = ").Append(Redacted(Password));
        builder.Append(", CertificateFile = ").Append(Redacted(CertificateFile));
        builder.Append(", CertificateFilePassword = ").Append(Redacted(CertificateFilePassword));
        builder.Append(", CertificatePrivateKeyFile = ").Append(Redacted(CertificatePrivateKeyFile));
        builder.Append(", SslProtocol = ").Append(SslProtocol);
        builder.Append(", DisableCertificateValidation = ").Append(DisableCertificateValidation);
        builder.Append(", CleanSession = ").Append(CleanSession);
        builder.Append(", SessionExpiryInterval = ").Append(SessionExpiryInterval);
        builder.Append(", WillTopic = ").Append(WillTopic);
        builder.Append(", WillMessage = ").Append(WillMessage);
        builder.Append(", WillRetain = ").Append(WillRetain);
        builder.Append(", Pooling = ").Append(Pooling);
        builder.Append(", MaxPendingMessages = ").Append(MaxPendingMessages);
        builder.Append(", DefaultSerializer = ").Append(DefaultSerializer);
        builder.Append(", BrokerReceiveMaximum = ").Append(BrokerReceiveMaximum);

        return true;
    }

    /// <summary>
    /// Whether a certificate is configured is worth knowing, its content is not: the value carries
    /// the private key itself when it is not a file name.
    /// </summary>
    private static string? Redacted(string? secret)
        => secret is null ? null : "***";
}
