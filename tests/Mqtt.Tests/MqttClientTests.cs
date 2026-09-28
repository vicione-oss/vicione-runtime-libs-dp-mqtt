using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using MQTTnet.Extensions;
using MQTTnet.Extensions.ManagedClient;
using NSubstitute;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttClient_Connect
{
    [Fact]
    public async Task Warns_about_an_unencrypted_connection_Async()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            Username = "user",
            Password = "s3cret",
        };
        FakeLogger logger = new();
        using MqttClient client = new(
            Substitute.For<IManagedMqttClient>(), communication.ToCommunicationInfo(), new MqttLogger(logger));

        await client.Connect();

        var warnings = logger.Collector.GetSnapshot().Where(r => r.Level == LogLevel.Warning).ToList();
        warnings.Should().HaveCount(2);
        warnings.Should().Contain(r => r.Message.Contains("not encrypted"));
        warnings.Should().Contain(r => r.Message.Contains("clear text"));
    }

    [Fact]
    public async Task Stays_silent_for_an_encrypted_connection_Async()
    {
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Host = "localhost",
            Username = "user",
            Password = "s3cret",
            CertificateAuthorityFile = TestCertificates.CertificateAuthorityPem,
        };
        FakeLogger logger = new();
        using MqttClient client = new(
            Substitute.For<IManagedMqttClient>(), communication.ToCommunicationInfo(), new MqttLogger(logger));

        await client.Connect();

        logger.Collector.GetSnapshot().Should().NotContain(r => r.Level == LogLevel.Warning);
    }
}
