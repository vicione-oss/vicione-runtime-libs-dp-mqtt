using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.Loader;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using MQTTnet;
using MQTTnet.Extensions;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using NSubstitute;
using ViciOne.ManagedEngine.ExternalCommunication;
using ViciOne.ManagedEngine.Runtime;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttDataPortOutgoing_
{
    [Fact]
    public async Task DependencyInjectionProviderFactory_can_create_instance_Async()
    {
        await using var providerFactory = new ConstructorProviderFactory();
        MqttDataPortCommunication communication = new();
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = "localhost",
        };

        var instance = providerFactory.CreateExternalOutgoing(
            communication,
            new(),
            Substitute.For<ILoggerFactory>(),
            Substitute.For<INameResolver>(),
            AssemblyLoadContext.Default);

        instance.Should().NotBeNull().And.BeOfType<MqttDataPortOutgoing>();
    }

    [Fact]
    public async Task Can_send_node_as_group_and_value_node_Async()
    {
        Node groupNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "group",
            ParentId = null,
            TransferredChannels = { "gv", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            ParentId = groupNode.Id,
            AffectedChannels = { "v", "gv", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, valueNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        var timestamp = DateTime.Now;
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "gv",
                Value = 23,
                Validity = 100,
                Timestamp = timestamp,
            },
            new()
            {
                Channel = "v",
                Value = 112,
                Validity = 100,
                Timestamp = timestamp,
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().HaveCount(2);
        messages.Should().SatisfyRespectively(
            m =>
            {
                m.Topic.Should().Be("group");
                JsonNode.Parse(m.ConvertPayloadToString())!["value"].Deserialize<int>().Should().Be(23);
            },
            m =>
            {
                m.Topic.Should().Be("group/value");
                JsonSerializer.Deserialize<int>(m.ConvertPayloadToString()).Should().Be(112);
                m.UserProperties.Should().BeNullOrEmpty();
            });
    }

    [Fact]
    public async Task Can_send_complex_group_node_Async()
    {
        Node groupNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "group",
            ParentId = null,
            TransferredChannels = { "gv1", "gv2", "gv3", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node value1Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value1",
            ParentId = groupNode.Id,
            AffectedChannels = { "gv1", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node subgroupNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "subgroup",
            ParentId = groupNode.Id,
            DesignId = MqttNodeDesignId.Topic,
        };
        Node value2Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value2",
            ParentId = subgroupNode.Id,
            AffectedChannels = { "gv2", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node value3Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value3",
            ParentId = subgroupNode.Id,
            AffectedChannels = { "gv3", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, subgroupNode, value1Node, value2Node, value3Node,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "gv1",
                Value = 23,
                Validity = 80,
                Timestamp = new(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            },
            new()
            {
                Channel = "gv2",
                Value = 24,
                Validity = 100,
                Timestamp = new(2023, 1, 1, 0, 1, 0, DateTimeKind.Utc),
            },
            new()
            {
                Channel = "gv3",
                Value = null,
                Validity = 25,
                Timestamp = new(2023, 1, 1, 0, 10, 0, DateTimeKind.Utc),
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        var message = messages.Should().ContainSingle().Which;
        message.Topic.Should().Be("group");
        message.UserProperties.Should().BeNullOrEmpty();
        message.ConvertPayloadToString().Should().Be(JsonSerializer.Serialize(JsonDocument.Parse($$"""
        {
            "value1": 23,
            "subgroup":
            {
                "value2": 24,
                "value3": null
            }
        }
        """)));
    }

    [Fact]
    public async Task Sends_with_QualityOfService_level_Async()
    {
        Node valueNode = new()
        {
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            QualityOfService = MqttQualityOfServiceLevel.ExactlyOnce,
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = 112,
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().ContainSingle().Which.QualityOfServiceLevel.Should().Be(MqttQualityOfServiceLevel.ExactlyOnce);
    }

    [Fact]
    public async Task Sends_with_retain_Async()
    {
        Node valueNode = new()
        {
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            Properties =
            {
                { MqttNodeProperties.Retain, new() { Value = true, } },
            }
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
        };
        _ = new MqttDataPortProperties(communication) { };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = 112,
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().ContainSingle().Which.Retain.Should().BeTrue();
    }

    [Fact]
    public async Task Can_handle_send_failure_Async()
    {
        Node valueNode = new()
        {
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = "local",
            Port = 23,
        };
        var client = Substitute.For<IVirtualMqttClient>();
        client.When(c => c.Publish(Arg.Any<MqttApplicationMessage>())).Throw<InvalidOperationException>();
        FakeLogger<MqttDataPortOutgoing> logger = new();
        using MqttDataPortOutgoing outgoing = new(communication, client, logger);
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = 112,
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        logger.Collector.Count.Should().Be(1);
        logger.LatestRecord.Exception.Should().BeOfType<InvalidOperationException>();
        logger.LatestRecord.Message.Should().MatchEquivalentOf("*fail*send*local*23*");
    }

    [Fact]
    public async Task Omits_user_properties_for_a_data_point_without_children_Async()
    {
        Node node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [node,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = 112,
                Validity = 100,
                Timestamp = DateTime.UtcNow,
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().ContainSingle().Which.UserProperties.Should().BeNullOrEmpty();
    }

    [Fact]
    public async Task Omits_meta_data_for_version_3_Async()
    {
        Node node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [node,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V311,
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        var timestamp = DateTime.Now;
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = 112,
                Validity = 100,
                Timestamp = timestamp,
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().ContainSingle().Which.UserProperties.Should().BeNull();
    }

    [Theory]
    [InlineData(0, "Test", "\"Test\"")]
    [InlineData(1, "Test", "\"Test\"")]
    [InlineData(2, "Test", "Test")]
    public async Task Can_send_single_node_with_serializer(byte serializer, object value, string result)
    {
        Node valueNode = new()
        {
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            Properties = new()
            {
                { MqttNodeProperties.Serializer, new() { Value = serializer, } },
            },
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
            Host = "local",
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = value,
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should()
            .ContainSingle()
            .Which.ConvertPayloadToString().Should().Be(result);
    }

    [Fact]
    public async Task Can_send_JsonObject_Async()
    {
        Node valueNode = new()
        {
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
            Host = "local",
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = new JsonObject() { { "Test", 112 }, },
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should()
            .ContainSingle()
            .Which.ConvertPayloadToString().Should().Be("{\"Test\":112}");
    }

    [Fact]
    public async Task Can_send_JsonObject_in_a_group_node_Async()
    {
        Node parentNode = new()
        {
            Name = "parent",
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            ParentId = parentNode.Id,
            Name = "myValue",
            AffectedChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [parentNode, valueNode,],
            Host = "local",
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = new JsonObject() { { "Test", 112 }, },
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should()
            .ContainSingle()
            .Which.ConvertPayloadToString().Should().Be("{\"myValue\":{\"Test\":112}}");
    }
}

public class MqttDataPortOutgoing_SendAsync
{
    [Fact]
    public async Task Publishes_a_plain_data_point_on_the_wire_Async()
    {
        Node plantNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "plant",
            DesignId = MqttNodeDesignId.Topic,
        };
        Node temperatureNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "temperature",
            ParentId = plantNode.Id,
            AffectedChannels = { "t", },
            TransferredChannels = { "t", },
            DesignId = MqttNodeDesignId.Topic,
            Properties =
            {
                { MqttNodeProperties.Retain, new() { Value = true, } },
            },
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [plantNode, temperatureNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V500,
            QualityOfService = MqttQualityOfServiceLevel.AtLeastOnce,
        };
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        await client.Publish(Arg.Do<MqttApplicationMessage>(messages.Add));
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "t",
                Value = 21.5,
                Validity = 100,
                Timestamp = new(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc),
            },
        ];

        await outgoing.SendAsync(7, values, TestContext.Current.CancellationToken);

        var message = messages.Should().ContainSingle().Which;
        message.Topic.Should().Be("plant/temperature");
        message.Payload.ToArray().Should().Equal(Encoding.UTF8.GetBytes("21.5"));
        message.QualityOfServiceLevel.Should().Be(MqttQualityOfServiceLevel.AtLeastOnce);
        message.Retain.Should().BeTrue();
        message.ContentType.Should().Be("application/json");
        message.PayloadFormatIndicator.Should().Be(MqttPayloadFormatIndicator.CharacterData);
    }
}
