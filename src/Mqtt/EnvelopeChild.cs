using System;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// A data point that addresses the envelope of its parent's message instead of a topic of its own.
/// </summary>
/// <param name="Node">The configured node.</param>
/// <param name="Kind">Whether the engine writes the value or the port derives it from the parent.</param>
/// <param name="Key">The key the value is carried under.</param>
/// <param name="Channel">The channel the engine reads the value from or writes it to.</param>
/// <param name="ValueType">The type the value is converted from and to, or <c>null</c> for a child
/// whose node declares none.</param>
internal sealed record EnvelopeChild(INode Node, EnvelopeChildKind Kind, string Key, string Channel, Type? ValueType)
{
    /// <summary>
    /// Whether <see cref="Key"/> is one this port defines rather than one the user typed, which
    /// decides how a received message is searched for it.
    /// </summary>
    internal bool HasReservedKey => Kind != EnvelopeChildKind.UserProperty;

    internal static EnvelopeChild Create(INode node, EnvelopeChildKind kind, string channel)
    {
        var (key, valueType) = Describe(kind);

        return new(node, kind, key ?? node.Name, channel, node.ValueType ?? valueType);
    }

    private static (string? Key, Type? ValueType) Describe(EnvelopeChildKind kind)
        => kind switch
        {
            EnvelopeChildKind.UserProperty => (null, typeof(string)),
            EnvelopeChildKind.Timestamp => (MqttUserProperties.Timestamp, typeof(DateTime)),
            EnvelopeChildKind.Validity => (MqttUserProperties.Validity, typeof(long)),
            EnvelopeChildKind.Type => (MqttUserProperties.Type, null),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown envelope child kind."),
        };
}
