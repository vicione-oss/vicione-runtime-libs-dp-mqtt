using System;
using System.Text;
using AwesomeAssertions;
using MQTTnet;
using MQTTnet.Extensions;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttEnvelopeCodec_Format
{
    [Fact]
    public void Writes_a_string_verbatim()
        => MqttEnvelopeCodec.Format("a value, unescaped").Should().Be("a value, unescaped");

    [Theory]
    [InlineData(true, "true")]
    [InlineData(false, "false")]
    public void Writes_a_bool_in_lower_case(bool value, string expected)
        => MqttEnvelopeCodec.Format(value).Should().Be(expected);

    [Fact]
    public void Writes_an_integer_in_decimal()
        => MqttEnvelopeCodec.Format(-9223372036854775808L).Should().Be("-9223372036854775808");

    [Fact]
    public void Writes_a_float_with_every_digit_it_takes_to_read_it_back()
        => MqttEnvelopeCodec.Format(0.1 + 0.2).Should().Be("0.30000000000000004");

    /// <summary>
    /// The text of each of these is what <c>MqttEnvelopeCodec_Parse.Reads_a_float</c> reads back,
    /// so the two halves of the conversion meet on the same literal rather than on each other.
    /// </summary>
    [Theory]
    [InlineData(0d, "0")]
    [InlineData(-1.5d, "-1.5")]
    [InlineData(double.MaxValue, "1.7976931348623157E+308")]
    [InlineData(double.Epsilon, "5E-324")]
    public void Writes_a_float_at_the_edges_of_its_range(double value, string expected)
        => MqttEnvelopeCodec.Format(value).Should().Be(expected);

    [Fact]
    public void Writes_a_date_in_utc()
        => MqttEnvelopeCodec.Format(new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc)).Should().Be("2026-03-04T05:06:07.0000000Z");

    [Fact]
    public void Writes_a_narrower_primitive_than_the_tree_declares()
        => MqttEnvelopeCodec.Format(42).Should().Be("42");

    [Fact]
    public void Writes_a_missing_value_as_an_empty_text()
        => MqttEnvelopeCodec.Format(null).Should().BeEmpty();
}

public class MqttEnvelopeCodec_Parse
{
    [Theory]
    [InlineData("plain text")]
    [InlineData("")]
    public void Reads_a_string_verbatim(string text)
        => MqttEnvelopeCodec.Parse(text, typeof(string)).Should().Be(text);

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void Reads_a_bool(string text, bool expected)
        => MqttEnvelopeCodec.Parse(text, typeof(bool)).Should().Be(expected);

    [Theory]
    [InlineData("0", 0L)]
    [InlineData("-1", -1L)]
    [InlineData("9223372036854775807", long.MaxValue)]
    [InlineData("-9223372036854775808", long.MinValue)]
    public void Reads_an_integer(string text, long expected)
        => MqttEnvelopeCodec.Parse(text, typeof(long)).Should().Be(expected);

    [Theory]
    [InlineData("0", 0d)]
    [InlineData("-1.5", -1.5d)]
    [InlineData("1.7976931348623157E+308", double.MaxValue)]
    [InlineData("5E-324", double.Epsilon)]
    public void Reads_a_float(string text, double expected)
        => MqttEnvelopeCodec.Parse(text, typeof(double)).Should().Be(expected);

    [Fact]
    public void Reads_a_float_that_needs_seventeen_digits()
        => MqttEnvelopeCodec.Parse("0.30000000000000004", typeof(double)).Should().Be(0.1 + 0.2);

    [Fact]
    public void Reads_a_date_in_utc()
        => MqttEnvelopeCodec.Parse("2026-03-04T05:06:07.8910000Z", typeof(DateTime))
            .Should().Be(new DateTime(2026, 3, 4, 5, 6, 7, 891, DateTimeKind.Utc));

    /// <summary>
    /// The kind carries the fix as much as the ticks do: the outgoing side reads a timestamp of an
    /// unspecified kind as local time, so one that came back unspecified would be republished moved
    /// by the offset of the host. An equality assertion compares ticks alone and would not see it.
    /// </summary>
    [Fact]
    public void Reads_a_date_with_a_non_utc_offset_as_the_same_instant_in_utc()
    {
        var timestamp = MqttEnvelopeCodec.Parse("2026-03-04T05:06:07.0000000+02:00", typeof(DateTime))
            .Should().BeOfType<DateTime>().Which;

        timestamp.Should().Be(new DateTime(2026, 3, 4, 3, 6, 7, DateTimeKind.Utc));
        timestamp.Kind.Should().Be(DateTimeKind.Utc);
    }

    /// <summary>
    /// A sender that is not this port writes plain ISO 8601, with any number of fractional digits
    /// or none, and with the zone as <c>Z</c>, as an offset, or left off to mean UTC.
    /// </summary>
    [Theory]
    [InlineData("2026-03-04T05:06:07Z")]
    [InlineData("2026-03-04T05:06:07")]
    [InlineData("2026-03-04T05:06:07.0Z")]
    [InlineData("2026-03-04T05:06:07.000Z")]
    [InlineData("2026-03-04T07:06:07+02:00")]
    [InlineData("2026-03-04T00:06:07-05:00")]
    public void Reads_a_date_written_as_plain_iso_8601(string text)
    {
        var timestamp = MqttEnvelopeCodec.Parse(text, typeof(DateTime)).Should().BeOfType<DateTime>().Which;

        timestamp.Should().Be(new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc));
        timestamp.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Theory]
    [InlineData("2026-03-04")]
    [InlineData("2026-03-04 05:06:07Z")]
    [InlineData("04/03/2026 05:06:07")]
    [InlineData("2026-03-04T05:06:07.12345678Z")]
    [InlineData("the day before")]
    public void Returns_nothing_for_a_text_that_is_not_a_point_in_time(string text)
        => MqttEnvelopeCodec.Parse(text, typeof(DateTime)).Should().BeNull();

    [Fact]
    public void Returns_nothing_for_a_text_the_declared_type_cannot_read()
        => MqttEnvelopeCodec.Parse("not a number", typeof(long)).Should().BeNull();

    [Fact]
    public void Returns_nothing_for_a_type_no_envelope_child_declares()
        => MqttEnvelopeCodec.Parse("1", typeof(Guid)).Should().BeNull();
}

public class MqttEnvelopeCodec_FormatTimestamp
{
    /// <summary>
    /// Version 1.0.0 wrote the fixed <c>Timestamp</c> property through
    /// <c>MqttApplicationMessageBuilder.WithUserProperty(string, DateTime)</c>. A subscriber built
    /// against those messages must keep reading the ones this version publishes.
    /// </summary>
    [Theory]
    [InlineData(DateTimeKind.Utc)]
    [InlineData(DateTimeKind.Local)]
    [InlineData(DateTimeKind.Unspecified)]
    public void Writes_the_bytes_version_1_0_0_wrote(DateTimeKind kind)
    {
        DateTime timestamp = new(2026, 3, 4, 5, 6, 7, 123, kind);

        MqttEnvelopeCodec.FormatTimestamp(timestamp).Should().Be(WrittenByVersion100(timestamp));
    }

    private static string WrittenByVersion100(DateTime timestamp)
    {
        var message = new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithUserProperty(MqttUserProperties.Timestamp, timestamp)
            .Build();

        return Encoding.UTF8.GetString(message.UserProperties.Should().ContainSingle().Which.ValueBuffer.Span);
    }
}

public class MqttEnvelopeCodec_FormatValidity
{
    [Theory]
    [InlineData(100, "100")]
    [InlineData(25, "25")]
    [InlineData(-1, "-1")]
    [InlineData(0, "0")]
    public void Writes_the_engine_validity_as_it_stands(int validity, string expected)
        => MqttEnvelopeCodec.FormatValidity(validity).Should().Be(expected);
}
