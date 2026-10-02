using System.Buffers;
using System.Text.Json;
using System.Text.Json.Nodes;
using MQTTnet;

namespace ViciOne.Suite.DataPort;

internal static class MqttPayloadHelper
{
    internal static JsonNode? GetMessagePayloadAsJsonNode(MqttApplicationMessage message)
        => message.Payload.IsEmpty ? null : ParseJsonNode(message.Payload);

    /// <summary>
    /// Parses <paramref name="data"/> as exactly one JSON value. Parsing from a reader stops after
    /// the first value, which would read <c>21,5</c> as <c>21</c>, so the reader is moved past it:
    /// anything but whitespace after the value throws a <see cref="JsonException"/>.
    /// </summary>
    internal static JsonNode? ParseJsonNode(ReadOnlySequence<byte> data)
    {
        var reader = new Utf8JsonReader(data);
        var node = JsonNode.Parse(ref reader);
        reader.Read();

        return node;
    }
}
