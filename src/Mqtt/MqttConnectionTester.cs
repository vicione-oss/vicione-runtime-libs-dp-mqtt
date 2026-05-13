using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using MQTTnet.Extensions;

namespace ViciOne.Suite.DataPort;

public sealed class MqttConnectionTester : IDataPortConnectionTester<MqttDataPortCommunication>
{
    public static async Task TestConnectionAsync(MqttDataPortCommunication communication)
    {
        var communicationInfo = communication.ToCommunicationInfo();
        using var client = MqttOptimizer.Instance.Register(communicationInfo, NullLoggerFactory.Instance);
        try
        {
            await client.InnerClient.InternalClient.ConnectAsync(communicationInfo.BuildClientOptions().ClientOptions);
            await client.InnerClient.InternalClient.DisconnectAsync(new());
        }
        catch (Exception ex)
        {
            throw new DataPortConnectionFailedException("Failed to connect to MQTT broker.", ex);
        }
    }
}
