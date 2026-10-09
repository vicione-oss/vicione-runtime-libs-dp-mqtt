using System;
using MQTTnet;
using MQTTnet.Extensions;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// Reads the value of an envelope child out of the message of its parent, the counterpart of what
/// the outgoing port writes into the envelope.
/// </summary>
internal static class MqttEnvelopeReader
{
    internal static EnvelopeReadResult TryRead(EnvelopeChild child, MqttApplicationMessage message, out object? value)
    {
        var property = message.UserProperties?.FindOptional(child.Key, GetComparison(child));

        if (property is null)
        {
            value = null;
            return EnvelopeReadResult.Missing;
        }

        value = MqttTextCodec.Parse(property.GetText(), child.ValueType);

        return value is null ? EnvelopeReadResult.Malformed : EnvelopeReadResult.Read;
    }

    /// <summary>
    /// The key of a user property is what the user typed into the tree, so it is matched exactly,
    /// while the reserved keys are matched the way the rest of the port reads them.
    /// </summary>
    private static StringComparison GetComparison(EnvelopeChild child)
        => child.HasReservedKey ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
}
