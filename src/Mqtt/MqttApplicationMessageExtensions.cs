using System.Text.Json;
using System.Text.Json.Nodes;
using MQTTnet;
using MQTTnet.Extensions;

namespace ViciOne.Suite.DataPort;

internal static class MqttApplicationMessageExtensions
{
    internal static JsonNode? GetPayloadAsJsonNode(this MqttApplicationMessage message)
        => MqttPayloadHelper.GetMessagePayloadAsJsonNode(message);

    /// <summary>
    /// The assembly-qualified name of the data type the sender named for the payload, or
    /// <c>null</c> when the message carries none.
    /// </summary>
    internal static string? ReadValueTypeName(this MqttApplicationMessage message)
        => message.UserProperties?.FindOptional(MqttUserProperties.Type)?.GetText();

    /// <summary>
    /// The data type names the sender put on the members of a group message, shaped like its
    /// payload, or <c>null</c> when the message carries no such map.
    /// </summary>
    internal static JsonObject? ReadMemberTypeNames(this MqttApplicationMessage message)
    {
        if (message.UserProperties?.FindOptional(MqttUserProperties.Type) is not { } property)
            return null;

        try
        {
            return JsonNode.Parse(property.GetText()) as JsonObject;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
