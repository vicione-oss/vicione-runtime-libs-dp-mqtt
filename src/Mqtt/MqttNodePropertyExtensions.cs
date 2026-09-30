using System;
using System.Collections.Generic;
using MQTTnet.Protocol;

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

        return Serializer.Inherited;
    }

    internal static MqttQualityOfServiceLevel? GetQualityOfService(this INode node)
    {
        if (!node.TryGetProperty(MqttNodeProperties.QualityOfServiceOverride, out var property))
            return null;

        var value = property.GetValue<byte>();
        return value switch
        {
            0 => null,
            1 => MqttQualityOfServiceLevel.AtMostOnce,
            2 => MqttQualityOfServiceLevel.AtLeastOnce,
            3 => MqttQualityOfServiceLevel.ExactlyOnce,
            _ => throw new NotSupportedException($"The quality of service override '{value}' is not supported.")
        };
    }

    internal static (MqttQualityOfServiceLevel Level, bool IsAmbiguous) ResolveQualityOfService(this IReadOnlyCollection<INode> nodes, MqttQualityOfServiceLevel inherited)
    {
        MqttQualityOfServiceLevel? resolved = null;
        var isAmbiguous = false;
        foreach (var node in nodes)
        {
            var level = node.GetQualityOfService() ?? inherited;
            isAmbiguous |= resolved is not null && resolved != level;
            if (resolved is null || level > resolved)
                resolved = level;
        }

        return (resolved ?? inherited, isAmbiguous);
    }
}
