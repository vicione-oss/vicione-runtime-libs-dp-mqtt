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
        Timestamp = message.GetTimestamp(timeProvider).UtcDateTime;
        Validity = message.GetValidity();
    }

    internal MqttApplicationMessage Message { get; }

    /// <summary>
    /// The point in time the message carries, or the time it was received. Always in UTC: the
    /// outgoing port writes it back with <see cref="MqttEnvelopeCodec.FormatTimestamp"/>, which
    /// reads a timestamp of an unspecified kind as local time and would move it by the offset of
    /// the host.
    /// </summary>
    internal DateTime Timestamp { get; }

    internal int Validity { get; }

    internal List<ExternalValue> Values { get; } = [];
}
