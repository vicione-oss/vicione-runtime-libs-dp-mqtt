using System;

namespace ViciOne.Suite.DataPort;

internal static class MqttDataPortCommunicationExtensions
{
    internal static string GetName(this MqttDataPortCommunication communication)
    {
        MqttDataPortProperties properties = new(communication);
        return properties.Protocol switch
        {
            MqttProtocol.Tcp => $"{properties.Host}@{properties.Port}",
            MqttProtocol.WebSocket => properties.Url.ToString(),
            _ => throw new NotSupportedException($"The MQTT communication protocol '{properties.Protocol}' is not supported."),
        };
    }
}
