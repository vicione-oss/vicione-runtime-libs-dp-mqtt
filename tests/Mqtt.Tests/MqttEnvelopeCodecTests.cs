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
    public void Round_trips_a_string(string value)
        => MqttEnvelopeCodec.Parse(MqttEnvelopeCodec.Format(value), typeof(string)).Should().Be(value);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Round_trips_a_bool(bool value)
        => MqttEnvelopeCodec.Parse(MqttEnvelopeCodec.Format(value), typeof(bool)).Should().Be(value);

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void Round_trips_an_integer(long value)
        => MqttEnvelopeCodec.Parse(MqttEnvelopeCodec.Format(value), typeof(long)).Should().Be(value);

    [Theory]
    [InlineData(0d)]
    [InlineData(-1.5d)]
    [InlineData(double.MaxValue)]
    [InlineData(double.Epsilon)]
    public void Round_trips_a_float(double value)
        => MqttEnvelopeCodec.Parse(MqttEnvelopeCodec.Format(value), typeof(double)).Should().Be(value);

    [Fact]
    public void Round_trips_a_float_that_needs_seventeen_digits()
        => MqttEnvelopeCodec.Parse(MqttEnvelopeCodec.Format(0.1 + 0.2), typeof(double)).Should().Be(0.1 + 0.2);

    [Fact]
    public void Round_trips_a_date()
    {
        DateTime value = new(2026, 3, 4, 5, 6, 7, 891, DateTimeKind.Utc);

        MqttEnvelopeCodec.Parse(MqttEnvelopeCodec.Format(value), typeof(DateTime)).Should().Be(value);
    }

    [Fact]
    public void Round_trips_a_date_with_a_non_utc_offset_as_the_same_instant()
    {
        var value = new DateTimeOffset(2026, 3, 4, 5, 6, 7, TimeSpan.FromHours(2)).LocalDateTime;

        MqttEnvelopeCodec.Parse(MqttEnvelopeCodec.Format(value), typeof(DateTime)).Should().Be(value.ToUniversalTime());
    }

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
