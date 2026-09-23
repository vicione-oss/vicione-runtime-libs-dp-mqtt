using System.Text;
using MQTTnet.Packets;

namespace ViciOne.Suite.DataPort;

internal static class MqttUserProperties
{
    internal const string Timestamp = "Timestamp";
    internal const string Validity = "Validity";
    internal const string Type = "Type";

    internal static string GetText(this MqttUserProperty property)
        => Encoding.UTF8.GetString(property.ValueBuffer.Span);
}
