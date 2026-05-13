using System;
using System.Text.Json.Nodes;
using MQTTnet;
using MQTTnet.Extensions;

namespace ViciOne.Suite.DataPort;

internal static class MqttApplicationMessageExtensions
{
    internal static JsonNode? GetPayloadAsJsonNode(this MqttApplicationMessage message)
        => (message.PayloadSegment.Array?.Length ?? 0) != 0 ? JsonNode.Parse(message.PayloadSegment.Array) : null;

    internal static DateTimeOffset GetTimestamp(this MqttApplicationMessage message, TimeProvider? timeProvider = null)
    {
        timeProvider ??= TimeProvider.System;
        return message.UserProperties?.FindOptional(MqttUserProperties.Timestamp)?.GetDateTimeOffset() ?? timeProvider.GetUtcNow();
    }

    internal static int GetValidity(this MqttApplicationMessage message)
        => message.UserProperties?.FindOptional(MqttUserProperties.Validity)?.Get<int>() ?? 1;
}
