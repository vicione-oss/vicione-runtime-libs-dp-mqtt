using System;
using System.Globalization;
using System.Text.Json.Nodes;
using MQTTnet.Packets;

namespace ViciOne.Suite.DataPort;

internal static class MqttUserProperties
{
    internal const string Timestamp = "Timestamp";
    internal const string Validity = "Validity";
    internal const string EngineCycle = "EngineCycle";
    internal const string Type = "Type";

    internal static DateTimeOffset GetDateTimeOffset(this MqttUserProperty userProperty)
        => DateTimeOffset.ParseExact(userProperty.Value, "O", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);

    internal static Type GetAsType(this MqttUserProperty property)
        => property.Value.ToType();

    internal static JsonObject? GetAsJsonObject(this MqttUserProperty property)
    {
        try
        {
            return JsonNode.Parse(property.Value)?.AsObject();
        }
        catch
        {
            return null;
        }
    }
}
