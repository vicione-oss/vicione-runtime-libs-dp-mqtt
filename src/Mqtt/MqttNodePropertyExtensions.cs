using System;

namespace ViciOne.Suite.DataPort;

internal static class MqttNodePropertyExtensions
{
    internal static Serializer GetSerializer(this INode node)
    {
        if (node.TryGetProperty(MqttNodeProperties.Serializer, out var property))
        {
            var value = property.GetValue<byte>();
            return value switch
            {
                0 => Serializer.Inherited,
                1 => Serializer.Json,
                2 => Serializer.PlainText,
                _ => throw new NotSupportedException($"The serializer '{value}' is not supported.")
            };
        }

        return Serializer.Json;
    }
}
