using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// Writes a date as UTC, in the text of the <c>Timestamp</c> user property, and reads one as UTC,
/// so neither the zone a sender chose nor the zone of the host shows in what this port forwards or
/// publishes. It reads every ISO 8601 date the framework reads, which unlike
/// <see cref="MqttTextCodec"/> includes a date without a time of day, as midnight.
/// </summary>
internal sealed class UtcDateTimeJsonConverter : JsonConverter<DateTime>
{
    public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var date = reader.GetDateTime();

        return date.Kind switch
        {
            // A sender that leaves the zone off means UTC, as it does for a received Timestamp.
            DateTimeKind.Unspecified => DateTime.SpecifyKind(date, DateTimeKind.Utc),
            // The framework reads an offset into the local time of the host. Read again with the
            // offset kept, the instant cannot shift where that local time is ambiguous.
            DateTimeKind.Local => reader.GetDateTimeOffset().UtcDateTime,
            _ => date,
        };
    }

    public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
        => writer.WriteStringValue(MqttTextCodec.Format(value));
}
