using System;
using AwesomeAssertions;
using MQTTnet.Protocol;
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

public class MqttNodePropertyExtensions_GetQualityOfService
{
    [Theory]
    [InlineData(0, null)]
    [InlineData(1, MqttQualityOfServiceLevel.AtMostOnce)]
    [InlineData(2, MqttQualityOfServiceLevel.AtLeastOnce)]
    [InlineData(3, MqttQualityOfServiceLevel.ExactlyOnce)]
    public void Should_return_quality_of_service(byte qualityOfServiceValue, MqttQualityOfServiceLevel? qualityOfService)
    {
        Node node = new()
        {
            Properties = new()
            {
                { MqttNodeProperties.QualityOfServiceOverride, new() { Value = qualityOfServiceValue } }
            },
        };

        var result = node.GetQualityOfService();

        result.Should().Be(qualityOfService);
    }

    [Fact]
    public void Should_inherit_by_default()
    {
        Node node = new();

        var result = node.GetQualityOfService();

        result.Should().BeNull();
    }

    [Fact]
    public void Should_throw_for_an_unsupported_value()
    {
        Node node = new()
        {
            Properties = new()
            {
                { MqttNodeProperties.QualityOfServiceOverride, new() { Value = (byte)4 } }
            },
        };

        FluentActions.Invoking(node.GetQualityOfService).Should().Throw<NotSupportedException>()
            .WithMessage("*quality of service*'4'*not*supported*");
    }
}

public class MqttNodePropertyExtensions_ResolveQualityOfService
{
    [Theory]
    [InlineData(1, 3, MqttQualityOfServiceLevel.ExactlyOnce)]
    [InlineData(3, 1, MqttQualityOfServiceLevel.ExactlyOnce)]
    [InlineData(2, 1, MqttQualityOfServiceLevel.AtLeastOnce)]
    public void Should_return_the_highest_requested_level(byte first, byte second, MqttQualityOfServiceLevel qualityOfService)
    {
        INode[] nodes = [Overriding(first), Overriding(second)];

        var (level, _) = nodes.ResolveQualityOfService(MqttQualityOfServiceLevel.AtMostOnce);

        level.Should().Be(qualityOfService);
    }

    [Theory]
    [InlineData(1, 3, true)]
    [InlineData(2, 2, false)]
    [InlineData(0, 2, false)]
    public void Should_report_ambiguity_from_the_effective_levels(byte first, byte second, bool isAmbiguous)
    {
        INode[] nodes = [Overriding(first), Overriding(second)];

        var (_, ambiguous) = nodes.ResolveQualityOfService(MqttQualityOfServiceLevel.AtLeastOnce);

        ambiguous.Should().Be(isAmbiguous);
    }

    [Fact]
    public void Should_return_the_inherited_level_without_nodes()
    {
        INode[] nodes = [];

        var (level, ambiguous) = nodes.ResolveQualityOfService(MqttQualityOfServiceLevel.ExactlyOnce);

        level.Should().Be(MqttQualityOfServiceLevel.ExactlyOnce);
        ambiguous.Should().BeFalse();
    }

    private static Node Overriding(byte qualityOfServiceOverride) => new()
    {
        Properties = new()
        {
            { MqttNodeProperties.QualityOfServiceOverride, new() { Value = qualityOfServiceOverride } }
        },
    };
}
