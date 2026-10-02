using System;
using System.Text;
using AwesomeAssertions;
using MQTTnet;
using MQTTnet.Extensions;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttTextCodec_Format
{
    [Fact]
    public void Writes_a_string_verbatim()
        => MqttTextCodec.Format("a value, unescaped").Should().Be("a value, unescaped");

    [Theory]
    [InlineData(true, "true")]
    [InlineData(false, "false")]
    public void Writes_a_bool_in_lower_case(bool value, string expected)
        => MqttTextCodec.Format(value).Should().Be(expected);

    [Fact]
    public void Writes_an_integer_in_decimal()
        => MqttTextCodec.Format(-9223372036854775808L).Should().Be("-9223372036854775808");

    [Fact]
    public void Writes_a_float_with_every_digit_it_takes_to_read_it_back()
        => MqttTextCodec.Format(0.1 + 0.2).Should().Be("0.30000000000000004");

    /// <summary>
    /// The text of each of these is what <c>MqttTextCodec_Parse.Reads_a_float</c> reads back,
    /// so the two halves of the conversion meet on the same literal rather than on each other.
    /// </summary>
    [Theory]
    [InlineData(0d, "0")]
    [InlineData(-1.5d, "-1.5")]
    [InlineData(double.MaxValue, "1.7976931348623157E+308")]
    [InlineData(double.Epsilon, "5E-324")]
    public void Writes_a_float_at_the_edges_of_its_range(double value, string expected)
        => MqttTextCodec.Format(value).Should().Be(expected);

    [Fact]
    public void Writes_a_single_precision_float_without_the_digits_of_a_double()
        => MqttTextCodec.Format(0.1f).Should().Be("0.1");

    [Fact]
    public void Writes_a_date_in_utc()
        => MqttTextCodec.Format(new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc)).Should().Be("2026-03-04T05:06:07.0000000Z");

    /// <summary>
    /// A received date without a zone is read as UTC, so one the engine wrote without a kind is sent
    /// as UTC too, instead of moved by the offset of the host. Only a host off UTC can tell.
    /// </summary>
    [Fact]
    public void Writes_a_date_of_unspecified_kind_as_utc()
        => MqttTextCodec.Format(new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified)).Should().Be("2026-03-04T05:06:07.0000000Z");

    [Fact]
    public void Writes_a_narrower_primitive_than_the_tree_declares()
        => MqttTextCodec.Format(42).Should().Be("42");

    [Fact]
    public void Writes_a_missing_value_as_an_empty_text()
        => MqttTextCodec.Format(null).Should().BeEmpty();
}

public class MqttTextCodec_Parse
{
    [Theory]
    [InlineData("plain text")]
    [InlineData("")]
    public void Reads_a_string_verbatim(string text)
        => MqttTextCodec.Parse(text, typeof(string)).Should().Be(text);

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void Reads_a_bool(string text, bool expected)
        => MqttTextCodec.Parse(text, typeof(bool)).Should().Be(expected);

    [Theory]
    [InlineData("0", 0L)]
    [InlineData("-1", -1L)]
    [InlineData("9223372036854775807", long.MaxValue)]
    [InlineData("-9223372036854775808", long.MinValue)]
    public void Reads_an_integer(string text, long expected)
        => MqttTextCodec.Parse(text, typeof(long)).Should().Be(expected);

    [Theory]
    [InlineData("0", 0d)]
    [InlineData("-1.5", -1.5d)]
    [InlineData("1.7976931348623157E+308", double.MaxValue)]
    [InlineData("5E-324", double.Epsilon)]
    public void Reads_a_float(string text, double expected)
        => MqttTextCodec.Parse(text, typeof(double)).Should().Be(expected);

    [Theory]
    [MemberData(nameof(GetIntegersAtTheEdgesOfTheirRange))]
    public void Reads_an_integer_of_every_width(string text, object expected)
        => MqttTextCodec.Parse(text, expected.GetType()).Should().Be(expected);

    public static TheoryData<string, object> GetIntegersAtTheEdgesOfTheirRange()
        => new()
        {
            { "-128", sbyte.MinValue },
            { "255", byte.MaxValue },
            { "-32768", short.MinValue },
            { "65535", ushort.MaxValue },
            { "-2147483648", int.MinValue },
            { "4294967295", uint.MaxValue },
            { "18446744073709551615", ulong.MaxValue },
        };

    [Theory]
    [InlineData("0x2C")]
    [InlineData("#2C")]
    [InlineData("2C")]
    public void Returns_nothing_for_an_integer_in_hex(string text)
        => MqttTextCodec.Parse(text, typeof(int)).Should().BeNull();

    [Fact]
    public void Returns_nothing_for_an_integer_beyond_the_range_of_its_width()
        => MqttTextCodec.Parse("128", typeof(sbyte)).Should().BeNull();

    [Fact]
    public void Reads_a_single_precision_float()
        => MqttTextCodec.Parse("0.1", typeof(float)).Should().Be(0.1f);

    [Theory]
    [InlineData("1e39", typeof(float))]
    [InlineData("-1e39", typeof(float))]
    [InlineData("1e309", typeof(double))]
    [InlineData("-1e309", typeof(double))]
    public void Returns_nothing_for_a_float_beyond_the_range_of_its_type(string text, Type type)
        => MqttTextCodec.Parse(text, type).Should().BeNull();

    [Theory]
    [InlineData("NaN", double.NaN)]
    [InlineData("Infinity", double.PositiveInfinity)]
    [InlineData("-Infinity", double.NegativeInfinity)]
    public void Reads_a_float_that_is_not_a_finite_number(string text, double expected)
        => MqttTextCodec.Parse(text, typeof(double)).Should().Be(expected);

    [Fact]
    public void Reads_a_float_that_needs_seventeen_digits()
        => MqttTextCodec.Parse("0.30000000000000004", typeof(double)).Should().Be(0.1 + 0.2);

    [Fact]
    public void Reads_a_date_in_utc()
        => MqttTextCodec.Parse("2026-03-04T05:06:07.8910000Z", typeof(DateTime))
            .Should().Be(new DateTime(2026, 3, 4, 5, 6, 7, 891, DateTimeKind.Utc));

    /// <summary>
    /// The kind carries the fix as much as the ticks do: the outgoing side reads a timestamp of an
    /// unspecified kind as local time, so one that came back unspecified would be republished moved
    /// by the offset of the host. An equality assertion compares ticks alone and would not see it.
    /// </summary>
    [Fact]
    public void Reads_a_date_with_a_non_utc_offset_as_the_same_instant_in_utc()
    {
        var timestamp = MqttTextCodec.Parse("2026-03-04T05:06:07.0000000+02:00", typeof(DateTime))
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
        var timestamp = MqttTextCodec.Parse(text, typeof(DateTime)).Should().BeOfType<DateTime>().Which;

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
        => MqttTextCodec.Parse(text, typeof(DateTime)).Should().BeNull();

    [Fact]
    public void Returns_nothing_for_a_text_the_declared_type_cannot_read()
        => MqttTextCodec.Parse("not a number", typeof(long)).Should().BeNull();

    [Fact]
    public void Returns_nothing_for_a_type_without_a_text_form()
        => MqttTextCodec.Parse("1", typeof(Guid)).Should().BeNull();
}

public class MqttTextCodec_FormatTimestamp
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

        MqttTextCodec.FormatTimestamp(timestamp).Should().Be(WrittenByVersion100(timestamp));
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

public class MqttTextCodec_FormatValidity
{
    [Theory]
    [InlineData(100, "100")]
    [InlineData(25, "25")]
    [InlineData(-1, "-1")]
    [InlineData(0, "0")]
    public void Writes_the_engine_validity_as_it_stands(int validity, string expected)
        => MqttTextCodec.FormatValidity(validity).Should().Be(expected);
}
