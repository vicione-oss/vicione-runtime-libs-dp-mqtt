using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class SerializerExtensions_Serialize
{
    [Theory]
    [MemberData(nameof(GetData))]
    [SuppressMessage("Usage", "xUnit1045:Avoid using TheoryData type arguments that might not be serializable", Justification = "The values are of arbitrary types and cannot be serialized")]
    public void Should_serialize(Serializer serializer, object? value, string? expected, Type type)
    {
        var result = serializer.Serialize(value, type, new());

        Encoding.UTF8.GetString(result).Should().Be(expected);
    }

    [Fact]
    public void Should_throw_exception_for_unsupported_serializer()
    {
        Action act = () => Serializer.Inherited.Serialize(null, typeof(string), new());

        act.Should().Throw<NotSupportedException>().WithMessage("*Inherited*not*supported*");
    }

    [Fact]
    public void Writes_a_json_string_with_the_characters_it_holds()
        => Encoding.UTF8.GetString(Serializer.Json.Serialize("äöü € <a> & 'b'", typeof(string), JsonSetup.PayloadOptions))
            .Should().Be("\"äöü € <a> & 'b'\"");

    [Fact]
    public void Escapes_what_a_json_string_cannot_hold()
        => Encoding.UTF8.GetString(Serializer.Json.Serialize("\"\\\n", typeof(string), JsonSetup.PayloadOptions))
            .Should().Be("""
                "\"\\\n"
                """);

    [Theory]
    [InlineData(DateTimeKind.Utc)]
    [InlineData(DateTimeKind.Unspecified)]
    public void Writes_a_json_date_in_utc(DateTimeKind kind)
        => Encoding.UTF8.GetString(Serializer.Json.Serialize(new DateTime(2026, 10, 2, 6, 0, 0, 500, kind), typeof(DateTime), JsonSetup.PayloadOptions))
            .Should().Be("\"2026-10-02T06:00:00.5000000Z\"");

    [Fact]
    public void Writes_a_local_json_date_as_the_same_instant_in_utc()
    {
        var local = new DateTime(2026, 10, 2, 6, 0, 0, DateTimeKind.Utc).ToLocalTime();

        var result = Serializer.Json.Serialize(local, typeof(DateTime), JsonSetup.PayloadOptions);

        Encoding.UTF8.GetString(result).Should().Be("\"2026-10-02T06:00:00.0000000Z\"");
    }

    [Theory]
    [InlineData(double.NaN, "\"NaN\"")]
    [InlineData(double.PositiveInfinity, "\"Infinity\"")]
    [InlineData(double.NegativeInfinity, "\"-Infinity\"")]
    [InlineData(double.MaxValue, "1.7976931348623157E+308")]
    public void Writes_a_json_float_that_is_not_a_finite_number_as_its_name(double value, string expected)
        => Encoding.UTF8.GetString(Serializer.Json.Serialize(value, typeof(double), JsonSetup.PayloadOptions)).Should().Be(expected);

    public static TheoryData<Serializer, object?, string?, Type> GetData()
        => new()
        {
            { Serializer.PlainText, "Test", "Test", typeof(string) },
            { Serializer.PlainText, 23, "23", typeof(int) },
            { Serializer.PlainText, 23.2, "23.2", typeof(double) },
            { Serializer.PlainText, null, string.Empty, typeof(string) },
            { Serializer.PlainText, true, "true", typeof(bool) },
            { Serializer.PlainText, new DateTime(2026, 10, 2, 6, 0, 0, 500, DateTimeKind.Utc), "2026-10-02T06:00:00.5000000Z", typeof(DateTime) },
            { Serializer.PlainText, new DateTime(2026, 10, 2, 0, 0, 0, DateTimeKind.Utc), "2026-10-02T00:00:00.0000000Z", typeof(DateTime) },
            { Serializer.Json, "Test", "\"Test\"", typeof(string) },
            { Serializer.Json, 23, "23", typeof(int) },
            { Serializer.Json, 23.2, "23.2", typeof(double) },
            { Serializer.Json, null, "null", typeof(string) },
            { Serializer.Json, string.Empty, "\"\"", typeof(string) },
        };
}

public class SerializerExtensions_Deserialize
{
    [Theory]
    [MemberData(nameof(GetData))]
    [SuppressMessage("Usage", "xUnit1045:Avoid using TheoryData type arguments that might not be serializable", Justification = "The values are of arbitrary types and cannot be serialized")]
    public void Should_deserialize(Serializer serializer, string value, object? expected, Type type)
    {
        var data = Encoding.UTF8.GetBytes(value);

        var result = serializer.Deserialize(data, type, new());

        result.Should().Be(expected);
    }

    [Fact]
    public void Should_throw_exception_for_unsupported_serializer()
    {
        Action act = () => Serializer.Inherited.Deserialize([0, 1], typeof(string), new());

        act.Should().Throw<NotSupportedException>().WithMessage("*Inherited*not*supported*");
    }

    [Fact]
    public void Reads_a_plain_text_date_as_the_same_instant_in_utc()
    {
        var data = Encoding.UTF8.GetBytes("2026-10-02T08:00:00.5+02:00");

        var result = Serializer.PlainText.Deserialize(data, typeof(DateTime), new()).Should().BeOfType<DateTime>().Which;

        result.Should().Be(new DateTime(2026, 10, 2, 6, 0, 0, 500, DateTimeKind.Utc));
        result.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Theory]
    [InlineData("0x2C", typeof(int))]
    [InlineData("#2C", typeof(int))]
    [InlineData("1e39", typeof(float))]
    [InlineData("1e309", typeof(double))]
    [InlineData("10/02/2026 08:00:00", typeof(DateTime))]
    [InlineData("yes", typeof(bool))]
    public void Rejects_a_plain_text_payload_its_type_does_not_read(string value, Type type)
    {
        var data = Encoding.UTF8.GetBytes(value);

        var act = () => Serializer.PlainText.Deserialize(data, type, new());

        act.Should().Throw<FormatException>();
    }

    [Theory]
    [InlineData("\"2026-10-02T08:00:00+02:00\"")]
    [InlineData("\"2026-10-02T06:00:00Z\"")]
    [InlineData("\"2026-10-02T06:00:00\"")]
    [InlineData("\"2026-10-02T06:00:00.0000000Z\"")]
    public void Reads_a_json_date_as_the_same_instant_in_utc(string value)
    {
        var data = Encoding.UTF8.GetBytes(value);

        var result = Serializer.Json.Deserialize(data, typeof(DateTime), JsonSetup.PayloadOptions).Should().BeOfType<DateTime>().Which;

        result.Should().Be(new DateTime(2026, 10, 2, 6, 0, 0, DateTimeKind.Utc));
        result.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public void Reads_a_json_date_without_a_time_of_day_as_midnight_utc()
    {
        var data = Encoding.UTF8.GetBytes("\"2026-10-02\"");

        var result = Serializer.Json.Deserialize(data, typeof(DateTime), JsonSetup.PayloadOptions);

        result.Should().Be(new DateTime(2026, 10, 2, 0, 0, 0, DateTimeKind.Utc));
    }

    [Theory]
    [InlineData("1e39", typeof(float))]
    [InlineData("1e309", typeof(double))]
    [InlineData("-1e309", typeof(double))]
    [InlineData("\"1.5\"", typeof(double))]
    public void Rejects_a_json_float_its_type_does_not_hold(string value, Type type)
    {
        var data = Encoding.UTF8.GetBytes(value);

        var act = () => Serializer.Json.Deserialize(data, type, JsonSetup.PayloadOptions);

        act.Should().Throw<JsonException>();
    }

    [Theory]
    [InlineData("\"NaN\"", double.NaN)]
    [InlineData("\"Infinity\"", double.PositiveInfinity)]
    [InlineData("\"-Infinity\"", double.NegativeInfinity)]
    [InlineData("3.4028234663852886E+38", 3.4028234663852886E+38)]
    public void Reads_a_json_float_by_its_name(string value, double expected)
    {
        var data = Encoding.UTF8.GetBytes(value);

        var result = Serializer.Json.Deserialize(data, typeof(double), JsonSetup.PayloadOptions);

        result.Should().Be(expected);
    }

    [Fact]
    public void Reads_a_single_precision_json_float_by_its_name()
        => Serializer.Json.Deserialize("\"NaN\""u8.ToArray(), typeof(float), JsonSetup.PayloadOptions).Should().Be(float.NaN);

    public static TheoryData<Serializer, string, object?, Type> GetData()
        => new()
        {
            { Serializer.PlainText, "Test", "Test", typeof(string) },
            { Serializer.PlainText, "23", 23, typeof(int) },
            { Serializer.PlainText, "23.2", 23.2, typeof(double) },
            { Serializer.PlainText, string.Empty, null, typeof(string) },
            { Serializer.PlainText, "true", true, typeof(bool) },
            { Serializer.PlainText, " 44 ", 44, typeof(int) },
            { Serializer.Json, "\"Test\"", "Test", typeof(string) },
            { Serializer.Json, "23", 23, typeof(int) },
            { Serializer.Json, "23.2", 23.2, typeof(double) },
            { Serializer.Json, "\"\"", string.Empty, typeof(string) },
            { Serializer.Json, "null", null, typeof(string) },
        };
}

public class SerializerExtensions_ToContentType
{
    [Fact]
    public void Should_return_JSON_content_type()
    {
        var result = Serializer.Json.ToContentType();

        result.Should().Be("application/json");
    }

    [Fact]
    public void Should_return_plain_text_content_type()
    {
        var result = Serializer.PlainText.ToContentType();

        result.Should().Be("text/plain");
    }

    [Fact]
    public void Should_throw_exception_for_unsupported_serializer()
    {
        Action act = () => Serializer.Inherited.ToContentType();

        act.Should().Throw<NotSupportedException>().WithMessage("*Inherited*not*supported*");
    }
}
