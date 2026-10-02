using System;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// Reads and writes a float the way <see cref="MqttTextCodec"/> does. A number too large for its
/// type is refused instead of read as infinity. <c>NaN</c>, <c>Infinity</c> and <c>-Infinity</c>
/// have no JSON number, so they travel as a string holding their name.
/// </summary>
internal sealed class RangeCheckedFloatJsonConverter<T> : JsonConverter<T>
    where T : struct, IFloatingPointIeee754<T>
{
    public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var number = reader.TokenType switch
        {
            JsonTokenType.Number => MqttTextCodec.Parse(ReadNumberText(ref reader), typeof(T)),
            JsonTokenType.String => MqttTextCodec.Parse(reader.GetString()!, typeof(T)) is T named && !T.IsFinite(named) ? named : null,
            _ => null,
        };

        return number is T value ? value : throw new JsonException($"The value is not a {typeof(T).Name}.");
    }

    public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
    {
        var text = MqttTextCodec.Format(value);

        if (T.IsFinite(value))
            writer.WriteRawValue(text);
        else
            writer.WriteStringValue(text);
    }

    private static string ReadNumberText(ref Utf8JsonReader reader)
        => reader.HasValueSequence ? Encoding.UTF8.GetString(reader.ValueSequence) : Encoding.UTF8.GetString(reader.ValueSpan);
}
