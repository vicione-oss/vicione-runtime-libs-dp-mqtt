using AwesomeAssertions;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttNodePropertyExtensions_GetSerializer
{
    [Theory]
    [InlineData(0, Serializer.Inherited)]
    [InlineData(1, Serializer.Json)]
    [InlineData(2, Serializer.PlainText)]
    public void Should_return_serializer(byte serializerValue, Serializer serializer)
    {
        Node node = new()
        {
            Properties = new()
            {
                { MqttNodeProperties.Serializer, new() { Value = serializerValue } }
            },
        };

        var result = node.GetSerializer();

        result.Should().Be(serializer);
    }

    [Fact]
    public void Should_inherit_by_default()
    {
        Node node = new();

        var result = node.GetSerializer();

        result.Should().Be(Serializer.Inherited);
    }
}
