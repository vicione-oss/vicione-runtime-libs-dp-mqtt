using System;
using System.Globalization;
using System.Text.Json.Nodes;
using MQTTnet;
using MQTTnet.Extensions;

namespace ViciOne.Suite.DataPort;

internal static class MqttApplicationMessageExtensions
{
    internal static JsonNode? GetPayloadAsJsonNode(this MqttApplicationMessage message)
        => MqttPayloadHelper.GetMessagePayloadAsJsonNode(message);

    internal static DateTimeOffset GetTimestamp(this MqttApplicationMessage message, TimeProvider? timeProvider = null)
    {
        timeProvider ??= TimeProvider.System;
        return message.ReadTimestamp() ?? timeProvider.GetUtcNow();
    }

    /// <summary>
    /// The timestamp the sender put on the message, or <c>null</c> when it carries none or one that
    /// is not a round-trip formatted point in time.
    /// </summary>
    internal static DateTime? ReadTimestamp(this MqttApplicationMessage message)
        => message.UserProperties?.FindOptional(MqttUserProperties.Timestamp) is { } property
            ? MqttEnvelopeCodec.Parse(property.GetText(), typeof(DateTime)) as DateTime?
            : null;

    /// <summary>
    /// The assembly-qualified name of the data type the sender named for the payload, or
    /// <c>null</c> when the message carries none.
    /// </summary>
    internal static string? ReadValueTypeName(this MqttApplicationMessage message)
        => message.UserProperties?.FindOptional(MqttUserProperties.Type)?.GetText();

    internal static int GetValidity(this MqttApplicationMessage message)
        => message.UserProperties?.FindOptional(MqttUserProperties.Validity) is { } property
            ? ReadValidity(property.GetText())
            : 1;

    /// <summary>
    /// The validity the sender put on the message: any integer, of which everything but zero means
    /// valid, or a boolean, which a sender that is not this port may write instead. A text that is
    /// neither is read as valid, the same as a message that carries no validity at all.
    /// </summary>
    private static int ReadValidity(string text)
    {
        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var validity))
            return validity;

        return bool.TryParse(text, out var flag) && !flag ? 0 : 1;
    }
}
