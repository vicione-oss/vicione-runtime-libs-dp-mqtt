using System;
using System.Buffers;
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
            Serializer.PlainText => Encoding.UTF8.GetBytes(MqttTextCodec.Format(value)),
            _ => throw new NotSupportedException($"The serializer '{serializer}' is not supported."),
        };

    /// <summary>
    /// Reads a payload as <paramref name="type"/>. Both serializers fail the same way: a payload the
    /// type cannot be read from throws, and the caller reports it as unreadable.
    /// </summary>
    /// <returns>
    /// <see langword="null"/> for an empty payload or a JSON <c>null</c>, which are values and not
    /// failures.
    /// </returns>
    /// <exception cref="JsonException">The JSON payload is not one value of <paramref name="type"/>.</exception>
    /// <exception cref="FormatException">The plain text payload is not a <paramref name="type"/>.</exception>
    internal static object? Deserialize(this Serializer serializer, byte[]? data, Type type, JsonSerializerOptions options)
    {
        if (data is null || data.Length == 0)
            return null;

        return serializer switch
        {
            Serializer.Json => JsonNode.Parse(data).Deserialize(type, options),
            Serializer.PlainText => ParsePlainText(Encoding.UTF8.GetString(data), type),
            _ => throw new NotSupportedException($"The serializer '{serializer}' is not supported."),
        };
    }

    /// <inheritdoc cref="Deserialize(Serializer, byte[], Type, JsonSerializerOptions)"/>
    internal static object? Deserialize(this Serializer serializer, ReadOnlySequence<byte> data, Type type, JsonSerializerOptions options)
    {
        if (data.Length == 0)
            return null;

        return serializer switch
        {
            Serializer.Json => MqttPayloadHelper.ParseJsonNode(data)?.Deserialize(type, options),
            Serializer.PlainText => ParsePlainText(Encoding.UTF8.GetString(data), type),
            _ => throw new NotSupportedException($"The serializer '{serializer}' is not supported."),
        };
    }

    private static object ParsePlainText(string text, Type type)
        => MqttTextCodec.Parse(text, type) ?? throw new FormatException($"The payload is not a {type.Name} in plain text.");

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
