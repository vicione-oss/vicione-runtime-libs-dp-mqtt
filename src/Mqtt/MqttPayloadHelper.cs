using System.Text.Json;
using System.Text.Json.Nodes;
using MQTTnet;

namespace ViciOne.Suite.DataPort;

internal static class MqttPayloadHelper
{
    internal static JsonNode? GetMessagePayloadAsJsonNode(MqttApplicationMessage message)
    {
        if (message.Payload.IsEmpty)
            return null;

        var reader = new Utf8JsonReader(message.Payload);

        return JsonNode.Parse(ref reader);
    }
}
