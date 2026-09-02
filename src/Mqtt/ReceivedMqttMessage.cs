using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using MQTTnet;
using MQTTnet.Extensions;
using MQTTnet.Packets;
using ViciOne.ManagedEngine.ExternalCommunication;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// One received message and everything read from its envelope, together with the values the port
/// derives from it.
/// </summary>
internal sealed class ReceivedMqttMessage
{
    internal ReceivedMqttMessage(MqttApplicationMessage message, TimeProvider timeProvider)
    {
        Message = message;
        Timestamp = message.GetTimestamp(timeProvider).DateTime;
        Validity = message.GetValidity();
        TypeProperty = message.UserProperties?.FindOptional(MqttUserProperties.Type);
        MetaJson = TypeProperty?.GetAsJsonObject();
    }

    internal MqttApplicationMessage Message { get; }

    internal DateTime Timestamp { get; }

    internal int Validity { get; }

    internal MqttUserProperty? TypeProperty { get; }

    internal JsonObject? MetaJson { get; }

    internal List<ExternalValue> Values { get; } = [];
}
