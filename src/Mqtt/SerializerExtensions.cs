using System;
using System.ComponentModel;
using System.Globalization;
using System.Net.Mime;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace ViciOne.Suite.DataPort;

internal static class SerializerExtensions
{
    internal static byte[] Serialize(this Serializer serializer, object? value, Type type, JsonSerializerOptions options)
        => serializer switch
        {
            Serializer.Json => JsonSerializer.SerializeToUtf8Bytes(value, type, options),
            Serializer.PlainText => Encoding.UTF8.GetBytes(value is not null ? TypeDescriptor.GetConverter(type).ConvertToString(null, CultureInfo.InvariantCulture, value) ?? string.Empty : string.Empty),
            _ => throw new NotSupportedException($"The serializer '{serializer}' is not supported."),
        };

    internal static object? Deserialize(this Serializer serializer, byte[]? data, Type type, JsonSerializerOptions options)
    {
        if (data is null || data.Length == 0)
            return null;

        return serializer switch
        {
            Serializer.Json => JsonNode.Parse(data).Deserialize(type, options),
            Serializer.PlainText => TypeDescriptor.GetConverter(type).ConvertFromInvariantString(Encoding.UTF8.GetString(data)),
            _ => throw new NotSupportedException($"The serializer '{serializer}' is not supported."),
        };
    }

    internal static string ToContentType(this Serializer serializer)
        => serializer switch
        {
            Serializer.Json => MediaTypeNames.Application.Json,
            Serializer.PlainText => MediaTypeNames.Text.Plain,
            _ => throw new NotSupportedException($"The serializer '{serializer}' is not supported."),
        };

    internal static Serializer ApplyDefault(this Serializer serializer, Serializer defaultSerializer)
        => serializer == Serializer.Inherited ? defaultSerializer : serializer;
}
