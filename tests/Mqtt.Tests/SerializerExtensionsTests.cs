using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;
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

    public static TheoryData<Serializer, object?, string?, Type> GetData()
        => new()
        {
            { Serializer.PlainText, "Test", "Test", typeof(string) },
            { Serializer.PlainText, 23, "23", typeof(int) },
            { Serializer.PlainText, 23.2, "23.2", typeof(double) },
            { Serializer.PlainText, null, string.Empty, typeof(string) },
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

    public static TheoryData<Serializer, string, object?, Type> GetData()
        => new()
        {
            { Serializer.PlainText, "Test", "Test", typeof(string) },
            { Serializer.PlainText, "23", 23, typeof(int) },
            { Serializer.PlainText, "23.2", 23.2, typeof(double) },
            { Serializer.PlainText, string.Empty, null, typeof(string) },
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
