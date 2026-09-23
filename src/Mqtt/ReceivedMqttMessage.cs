using System;
using System.Collections.Generic;
using System.Globalization;
using MQTTnet;
using MQTTnet.Extensions;
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

        var property = message.UserProperties?.FindOptional(MqttUserProperties.Timestamp);
        var sent = property is null ? null : MqttEnvelopeCodec.Parse(property.GetText(), typeof(DateTime)) as DateTime?;

        Timestamp = sent ?? timeProvider.GetUtcNow().UtcDateTime;
        UnreadableTimestamp = property is not null && sent is null ? property.GetText() : null;

        var validityProperty = message.UserProperties?.FindOptional(MqttUserProperties.Validity);
        var validity = validityProperty is null ? null : ReadValidity(validityProperty.GetText());

        Validity = validityProperty is null ? 1 : validity ?? 0;
        UnreadableValidity = validityProperty is not null && validity is null ? validityProperty.GetText() : null;
    }

    internal MqttApplicationMessage Message { get; }

    /// <summary>
    /// The point in time the message carries, or the time it was received. Always in UTC: the
    /// outgoing port writes it back with <see cref="MqttEnvelopeCodec.FormatTimestamp"/>, which
    /// reads a timestamp of an unspecified kind as local time and would move it by the offset of
    /// the host.
    /// </summary>
    internal DateTime Timestamp { get; }

    /// <summary>
    /// The text of a point in time the sender named but this port could not read, or <c>null</c>
    /// when the message carries a readable one or none at all. <see cref="Timestamp"/> is then the
    /// receive time standing in for a point in time the message did name, which is worth saying out
    /// loud: it is plausible and wrong, where a message that names no time at all is only missing
    /// one.
    /// </summary>
    internal string? UnreadableTimestamp { get; }

    internal int Validity { get; }

    /// <summary>
    /// The text of a validity the sender wrote but this port could not read, or <c>null</c> when the
    /// message carries a readable one or none at all. <see cref="Validity"/> is then 0: a value is
    /// never read as more valid than its sender may have said.
    /// </summary>
    internal string? UnreadableValidity { get; }

    internal List<ExternalValue> Values { get; } = [];

    /// <summary>
    /// The validity the sender put on the message: any integer, of which everything but zero means
    /// valid, or a boolean, which a sender that is not this port may write instead.
    /// </summary>
    private static int? ReadValidity(string text)
    {
        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var validity))
            return validity;

        if (bool.TryParse(text, out var flag))
            return flag ? 1 : 0;

        return null;
    }
}
