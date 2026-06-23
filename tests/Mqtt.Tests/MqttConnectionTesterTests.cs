using System.Threading.Tasks;
using AwesomeAssertions;
using MQTTnet.Server;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttConnectionTester_TestConnectionAsync
{
    [Fact]
    public async Task Can_connect_to_server()
    {
        var mqttServer = new MqttServerFactory().CreateMqttServer(new MqttServerOptionsBuilder().WithDefaultEndpoint().Build());
        await mqttServer.StartAsync();

        MqttDataPortCommunication communication = new()
        {
            Host = "localhost",
            Port = 1883,
            ProtocolVersion = 1,
        };

        try
        {
            await MqttConnectionTester.TestConnectionAsync(communication);
        }
        finally
        {
            await mqttServer.StopAsync();
        }
    }

    [Fact]
    public async Task Throws_if_test_connection_fails()
    {
        MqttDataPortCommunication communication = new()
        {
            Host = "localhost",
            Port = 1883,
            ProtocolVersion = 1,
        };

        var act = FluentActions.Awaiting(() => MqttConnectionTester.TestConnectionAsync(communication));

        await act.Should().ThrowAsync<DataPortConnectionFailedException>().WithMessage("*fail*connect*mqtt*");
    }
}
