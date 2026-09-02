using System;
using System.Collections.Generic;
using MQTTnet;
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
    }

    internal MqttApplicationMessage Message { get; }

    internal DateTime Timestamp { get; }

    internal int Validity { get; }

    internal List<ExternalValue> Values { get; } = [];
}
