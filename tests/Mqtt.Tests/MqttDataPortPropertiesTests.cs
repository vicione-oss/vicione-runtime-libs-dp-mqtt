using System;
using System.Security.Authentication;
using AwesomeAssertions;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttDataPortProperties_ClientId
{
    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { ClientId = value, });

        properties.ClientId.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]

    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            ClientId = value,
        };

        communication.ClientId.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "user" },
    };
}

public class MqttDataPortProperties_WillTopic
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.WillTopic.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { WillTopic = value, });

        properties.WillTopic.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            WillTopic = value,
        };

        communication.WillTopic.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "will" },
    };
}

public class MqttDataPortProperties_WillMessage
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.WillMessage.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { WillMessage = value, });

        properties.WillMessage.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            WillMessage = value,
        };

        communication.WillMessage.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "message" },
    };
}

public class MqttDataPortProperties_WillRetain
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.WillRetain.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(bool? value)
    {
        MqttDataPortProperties properties = new(new() { WillRetain = value, });

        properties.WillRetain.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(bool? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            WillRetain = value,
        };

        communication.WillRetain.Should().Be(value);
    }

    public static TheoryData<bool?> Conversion() => new()
    {
        { null! },
        { false },
        { true },
    };
}

public class MqttDataPortProperties_Protocol
{
    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(byte communicationValue, MqttProtocol propertyValue)
    {
        MqttDataPortProperties properties = new(new() { Protocol = communicationValue, });

        properties.Protocol.Should().Be(propertyValue);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(byte communicationValue, MqttProtocol propertyValue)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            Protocol = propertyValue,
        };

        communication.Protocol.Should().Be(communicationValue);
    }

    public static TheoryData<byte, MqttProtocol> Conversion() => new()
    {
        { 0, MqttProtocol.Tcp },
        { 1, MqttProtocol.WebSocket },
    };
}

public class MqttDataPortProperties_Url
{
    [Fact]
    public void Throws_on_TCP()
    {
        MqttDataPortProperties properties = new(new())
        {
            Protocol = MqttProtocol.Tcp,
        };

        FluentActions.Invoking(() => properties.Url).Should().Throw<InvalidOperationException>()
            .WithMessage("*property*Url*only*supported*protocol*WebSocket*");
    }

    [Fact]
    public void Throws_on_null_on_WebSocket()
    {
        MqttDataPortProperties properties = new(new() { Url = default, })
        {
            Protocol = MqttProtocol.WebSocket,
        };

        FluentActions.Invoking(() => properties.Url).Should().Throw<InvalidOperationException>()
            .WithMessage("*property*Url*no value*");
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string value)
    {
        MqttDataPortProperties properties = new(new() { Url = new(value), })
        {
            Protocol = MqttProtocol.WebSocket,
        };

        properties.Url.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]

    public void Setter_sets_value(string value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            Url = new Uri(value),
        };

        communication.Url.Should().Be(value);
    }

    public static TheoryData<string> Conversion() => new()
    {
        { "ws://localhost:9001/" },
    };
}

public class MqttDataPortProperties_Host
{
    [Fact]
    public void Throws_on_WebSocket()
    {
        MqttDataPortProperties properties = new(new())
        {
            Protocol = MqttProtocol.WebSocket,
        };

        FluentActions.Invoking(() => properties.Host).Should().Throw<InvalidOperationException>()
            .WithMessage("*property*Host*only*supported*protocol*Tcp*");
    }

    [Fact]
    public void Throws_on_null_on_TCP()
    {
        MqttDataPortProperties properties = new(new() { Host = default, })
        {
            Protocol = MqttProtocol.Tcp,
        };

        FluentActions.Invoking(() => properties.Host).Should().Throw<InvalidOperationException>()
            .WithMessage("*property*Host*no value*");
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string value)
    {
        MqttDataPortProperties properties = new(new() { Host = value, })
        {
            Protocol = MqttProtocol.Tcp,
        };

        properties.Host.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]

    public void Setter_sets_value(string value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            Host = value,
        };

        communication.Host.Should().Be(value);
    }

    public static TheoryData<string> Conversion() => new()
    {
        { new(string.Empty) },
        { new("localhost") },
    };
}

public class MqttDataPortProperties_Port
{
    [Fact]
    public void Throws_on_WebSocket()
    {
        MqttDataPortProperties properties = new(new())
        {
            Protocol = MqttProtocol.WebSocket,
        };

        FluentActions.Invoking(() => properties.Port).Should().Throw<InvalidOperationException>()
            .WithMessage("*property*Port*only*supported*protocol*Tcp*");
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(ushort? communicationValue, int? propertyValue)
    {
        MqttDataPortProperties properties = new(new() { Port = communicationValue, })
        {
            Protocol = MqttProtocol.Tcp,
        };

        properties.Port.Should().Be(propertyValue);
    }

    [Theory]
    [MemberData(nameof(Conversion))]

    public void Setter_sets_value(ushort? communicationValue, int? propertyValue)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            Port = propertyValue,
        };

        communication.Port.Should().Be(communicationValue);
    }

    public static TheoryData<ushort?, int?> Conversion() => new()
    {
        { null, null },
        { 1883, 1883 },
    };
}

public class MqttDataPortProperties_Username
{
    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { Username = value, });

        properties.Username.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]

    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            Username = value,
        };

        communication.Username.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "user" },
    };
}

public class MqttDataPortProperties_Password
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.Password.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { Password = value, });

        properties.Password.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            Password = value,
        };

        communication.Password.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "pass" },
    };
}

public class MqttDataPortProperties_QualityOfService
{
    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(byte communicationValue, MqttQualityOfServiceLevel propertyValue)
    {
        MqttDataPortProperties properties = new(new() { QualityOfService = communicationValue, });

        properties.QualityOfService.Should().Be(propertyValue);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(byte communicationValue, MqttQualityOfServiceLevel propertyValue)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            QualityOfService = propertyValue,
        };

        communication.QualityOfService.Should().Be(communicationValue);
    }

    public static TheoryData<byte, MqttQualityOfServiceLevel> Conversion() => new()
    {
        { 0, MqttQualityOfServiceLevel.AtMostOnce },
        { 1, MqttQualityOfServiceLevel.AtLeastOnce },
        { 2, MqttQualityOfServiceLevel.ExactlyOnce },
    };
}

public class MqttDataPortProperties_WillQualityOfService
{
    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(byte? communicationValue, MqttQualityOfServiceLevel propertyValue)
    {
        MqttDataPortProperties properties = new(new() { WillQualityOfService = communicationValue, });

        properties.WillQualityOfService.Should().Be(propertyValue);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(byte? communicationValue, MqttQualityOfServiceLevel propertyValue)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            WillQualityOfService = propertyValue,
        };

        communication.WillQualityOfService.Should().Be(communicationValue);
    }

    [Fact]
    public void Returns_at_most_once_if_missing()
    {
        MqttDataPortProperties properties = new(new() { WillQualityOfService = null, });

        properties.WillQualityOfService.Should().Be(MqttQualityOfServiceLevel.AtMostOnce);
    }

    public static TheoryData<byte?, MqttQualityOfServiceLevel> Conversion() => new()
    {
        { 0, MqttQualityOfServiceLevel.AtMostOnce },
        { 1, MqttQualityOfServiceLevel.AtLeastOnce },
        { 2, MqttQualityOfServiceLevel.ExactlyOnce },
    };
}

public class MqttDataPortProperties_ProtocolVersion
{
    [Fact]
    public void Getter_throws_for_unsupported_value()
    {
        MqttDataPortProperties properties = new(new() { ProtocolVersion = 2, });

        FluentActions.Invoking(() => properties.ProtocolVersion).Should().Throw<NotSupportedException>()
            .WithMessage("*ProtocolVersion*'2'*not*supported*");
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(byte communicationValue, MqttProtocolVersion propertyValue)
    {
        MqttDataPortProperties properties = new(new() { ProtocolVersion = communicationValue, });

        properties.ProtocolVersion.Should().Be(propertyValue);
    }

    [Fact]
    public void Setter_throws_for_unsupported_value()
    {
        MqttDataPortProperties properties = new(new());

        FluentActions.Invoking(() => properties.ProtocolVersion = MqttProtocolVersion.V310).Should().Throw<NotSupportedException>()
            .WithMessage("*ProtocolVersion*'V310'*not*supported*");
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(byte communicationValue, MqttProtocolVersion propertyValue)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = propertyValue,
        };

        communication.ProtocolVersion.Should().Be(communicationValue);
    }

    public static TheoryData<byte, MqttProtocolVersion> Conversion() => new()
    {
        { 0, MqttProtocolVersion.V311 },
        { 1, MqttProtocolVersion.V500 },
    };
}

public class MqttDataPortProperties_CertificateFile
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.CertificateFile.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { CertificateFile = value, });

        properties.CertificateFile.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            CertificateFile = value,
        };

        communication.CertificateFile.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "cert" },
    };
}

public class MqttDataPortProperties_CertificateFilePassword
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.CertificateFilePassword.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { CertificateFilePassword = value, });

        properties.CertificateFilePassword.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            CertificateFilePassword = value,
        };

        communication.CertificateFilePassword.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "certpass" },
    };
}

public class MqttDataPortProperties_CertificatePrivateKeyFile
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.CertificatePrivateKeyFile.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { CertificatePrivateKeyFile = value, });

        properties.CertificatePrivateKeyFile.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            CertificatePrivateKeyFile = value,
        };

        communication.CertificatePrivateKeyFile.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "certkey" },
    };
}

public class MqttDataPortProperties_CertificateAuthorityFile
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.CertificateAuthorityFile.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(string? value)
    {
        MqttDataPortProperties properties = new(new() { CertificateAuthorityFile = value, });

        properties.CertificateAuthorityFile.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(string? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            CertificateAuthorityFile = value,
        };

        communication.CertificateAuthorityFile.Should().Be(value);
    }

    public static TheoryData<string?> Conversion() => new()
    {
        { null! },
        { string.Empty },
        { "ca.pem" },
    };
}

public class MqttDataPortProperties_TlsMode
{
    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(byte? communicationValue, SslProtocols value)
    {
        MqttDataPortProperties properties = new(new() { TlsMode = communicationValue, });

        properties.TlsMode.Should().Be(value);
    }

    [Fact]
    public void Getter_throws_for_unsupported_value()
    {
        MqttDataPortProperties properties = new(new() { TlsMode = 4, });

        FluentActions.Invoking(() => properties.TlsMode).Should().Throw<NotSupportedException>()
            .WithMessage("*TlsMode*'4'*not*supported*");
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(byte? communicationValue, SslProtocols value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            TlsMode = value,
        };

        communication.TlsMode.Should().Be(communicationValue);
    }

    [Fact]
    public void Setter_throws_for_unsupported_value()
    {
        MqttDataPortProperties properties = new(new());

#pragma warning disable SYSLIB0039 // The obsolete TLS version is the rejected input
#pragma warning disable CA5397 // The obsolete TLS version is the rejected input
        FluentActions.Invoking(() => properties.TlsMode = SslProtocols.Tls).Should().Throw<NotSupportedException>()
            .WithMessage("*TlsMode*'Tls'*not*supported*");
#pragma warning restore CA5397 // The obsolete TLS version is the rejected input
#pragma warning restore SYSLIB0039 // The obsolete TLS version is the rejected input
    }

#pragma warning disable CA5398 // Hartcodierte SslProtocols-Werte vermeiden
    [Fact]
    public void Requires_TLS_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.TlsMode.Should().Be(SslProtocols.Tls12 | SslProtocols.Tls13);
    }

#pragma warning disable CA5398 // The TLS versions the ruleset offers
    public static TheoryData<byte?, SslProtocols> Conversion() => new()
    {
        { 0, SslProtocols.None },
        { 1, SslProtocols.Tls12 | SslProtocols.Tls13 },
        { 2, SslProtocols.Tls12 },
        { 3, SslProtocols.Tls13 },
    };
#pragma warning restore CA5398 // The TLS versions the ruleset offers
}

public class MqttDataPortProperties_LastWillEnabled
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.LastWillEnabled.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(bool? value)
    {
        MqttDataPortProperties properties = new(new() { LastWillEnabled = value, });

        properties.LastWillEnabled.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(bool? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            LastWillEnabled = value,
        };

        communication.LastWillEnabled.Should().Be(value);
    }

    public static TheoryData<bool?> Conversion() => new()
    {
        { null! },
        { false },
        { true },
    };
}

public class MqttDataPortProperties_ValidateCertificateChain
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.ValidateCertificateChain.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(bool? value)
    {
        MqttDataPortProperties properties = new(new() { ValidateCertificateChain = value, });

        properties.ValidateCertificateChain.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(bool? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            ValidateCertificateChain = value,
        };

        communication.ValidateCertificateChain.Should().Be(value);
    }

    public static TheoryData<bool?> Conversion() => new()
    {
        { null! },
        { false },
        { true },
    };
}

public class MqttDataPortProperties_CleanSession
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.CleanSession.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(bool? value)
    {
        MqttDataPortProperties properties = new(new() { CleanSession = value, });

        properties.CleanSession.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(bool? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            CleanSession = value,
        };

        communication.CleanSession.Should().Be(value);
    }

    public static TheoryData<bool?> Conversion() => new()
    {
        { null! },
        { false },
        { true },
    };
}

public class MqttDataPortProperties_SessionExpiryInterval
{
    [Fact]
    public void Returns_null_if_missing()
    {
        MqttDataPortProperties properties = new(new());

        properties.SessionExpiryInterval.Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(uint? value)
    {
        MqttDataPortProperties properties = new(new() { SessionExpiryInterval = value, });

        properties.SessionExpiryInterval.Should().Be(value);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(uint? value)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            SessionExpiryInterval = value,
        };

        communication.SessionExpiryInterval.Should().Be(value);
    }

    public static TheoryData<uint?> Conversion() => new()
    {
        { null! },
        { 0 },
        { 1 },
    };
}

public class MqttDataPortProperties_DefaultSerializer
{
    [Theory]
    [MemberData(nameof(Conversion))]
    public void Getter_returns_value(byte? value, Serializer serializer)
    {
        MqttDataPortProperties properties = new(new() { DefaultSerializer = value, });

        properties.DefaultSerializer.Should().Be(serializer);
    }

    [Theory]
    [MemberData(nameof(Conversion))]
    public void Setter_sets_value(byte? value, Serializer serializer)
    {
        MqttDataPortCommunication communication = new();

        _ = new MqttDataPortProperties(communication)
        {
            DefaultSerializer = serializer,
        };

        communication.DefaultSerializer.Should().Be(value);
    }

    [Fact]
    public void Returns_JSON_as_default()
    {
        MqttDataPortProperties properties = new(new() { DefaultSerializer = null, });

        properties.DefaultSerializer.Should().Be(Serializer.Json);
    }

    [Fact]
    public void Throws_on_unsupported_serializer()
    {
        MqttDataPortProperties properties = new(new() { DefaultSerializer = 0, });

        FluentActions.Invoking(() => properties.DefaultSerializer).Should().Throw<NotSupportedException>()
            .WithMessage("*serializer*'0'*not*supported*");
    }

    [Fact]
    public void Setter_throws_for_the_inherited_serializer()
    {
        MqttDataPortProperties properties = new(new());

        FluentActions.Invoking(() => properties.DefaultSerializer = Serializer.Inherited).Should().Throw<NotSupportedException>()
            .WithMessage("*DefaultSerializer*Inherited*not*supported*");
    }

    public static TheoryData<byte?, Serializer> Conversion() => new()
    {
        { 1, Serializer.Json },
    };
}
