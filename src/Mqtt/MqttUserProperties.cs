using System;
using System.Text;
using System.Text.Json.Nodes;
using MQTTnet.Packets;

namespace ViciOne.Suite.DataPort;

internal static class MqttUserProperties
{
    internal const string Timestamp = "Timestamp";
    internal const string Validity = "Validity";
    internal const string EngineCycle = "EngineCycle";
    internal const string Type = "Type";

    internal static string GetText(this MqttUserProperty property)
        => Encoding.UTF8.GetString(property.ValueBuffer.Span);

    internal static Type GetAsType(this MqttUserProperty property)
        => property.GetText().ToType();

    internal static JsonObject? GetAsJsonObject(this MqttUserProperty property)
    {
        try
        {
            return JsonNode.Parse(property.GetText())?.AsObject();
        }
        catch
        {
            return null;
        }
    }
}
