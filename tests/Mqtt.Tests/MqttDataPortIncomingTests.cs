using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.Loader;
using System.Text;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Time.Testing;
using MQTTnet;
using MQTTnet.Extensions;
using MQTTnet.Protocol;
using NSubstitute;
using ViciOne.ManagedEngine.ExternalCommunication;
using ViciOne.ManagedEngine.Runtime;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttDataPortIncoming_
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

        var instance = providerFactory.CreateExternalIncoming(
            communication,
            new(),
            Substitute.For<ILoggerFactory>(),
            Substitute.For<INameResolver>(),
            AssemblyLoadContext.Default);

        instance.Should().NotBeNull().And.BeOfType<MqttDataPortIncoming>();
    }

    [Fact]
    public async Task Connects_and_disconnects_correctly_Async()
    {
        Node groupNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "group",
            DesignId = MqttNodeDesignId.Topic,
        };
        Node value1Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value1",
            ParentId = groupNode.Id,
            TransferredChannels = { "v1", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node value2Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value2",
            ParentId = groupNode.Id,
            TransferredChannels = { "v2", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, value1Node, value2Node,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            QualityOfService = MqttQualityOfServiceLevel.AtLeastOnce,
        };

        var client = Substitute.For<IVirtualMqttClient>();
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);

        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        await client.Received(1).Connect();
        await client.Received(1).Subscribe("group/value1", MqttQualityOfServiceLevel.AtLeastOnce, false);
        await client.Received(1).Subscribe("group/value2", MqttQualityOfServiceLevel.AtLeastOnce, false);

        await incoming.DisconnectAsync(TestContext.Current.CancellationToken);
        await client.Received(1).Disconnect();
        await client.Received(1).Unsubscribe("group/value1");
        await client.Received(1).Unsubscribe("group/value2");
    }

    [Fact]
    public async Task Can_receive_group_node_Async()
    {
        Node groupNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "group",
            ParentId = null,
            TransferredChannels = { "gv1", "gv2", },
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
        Node value2Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value2",
            ParentId = groupNode.Id,
            AffectedChannels = { "gv2", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, value1Node, value2Node,],
            Host = "local",
        };

        var client = Substitute.For<IVirtualMqttClient>();
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        var message = new MqttApplicationMessageBuilder()
            .WithTopic("group")
            .WithPayload("""
            {
                "value1": 23,
                "value2": 25
            }
            """)
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes($$"""
            {
                "value1":"{{typeof(int).AssemblyQualifiedName}}",
                "value2": "{{typeof(int).AssemblyQualifiedName}}"
            }
            """))
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        messages.Should().HaveCount(2);
        messages.Should().SatisfyRespectively(
            m =>
            {
                m.Channel.Should().Be("gv1");
                m.Value.Should().Be(23);
            },
            m =>
            {
                m.Channel.Should().Be("gv2");
                m.Value.Should().Be(25);
            });
    }

    [Fact]
    public async Task Can_receive_group_node_without_type_Async()
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
            AffectedChannels = { "gv", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(int),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, valueNode,],
            Host = "local",
        };

        var client = Substitute.For<IVirtualMqttClient>();
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        var message = new MqttApplicationMessageBuilder()
            .WithTopic("group")
            .WithPayload("""
            {
                "value": 23
            }
            """)
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        var result = messages.Should().ContainSingle().Which;
        result.Channel.Should().Be("gv");
        result.Value.Should().Be(23);
    }

    [Fact]
    public async Task Can_receive_part_of_a_group_node_Async()
    {
        Node groupNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "group",
            ParentId = null,
            TransferredChannels = { "gv1", "gv2", },
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
        Node value2Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value2",
            ParentId = groupNode.Id,
            AffectedChannels = { "gv2", },
            DesignId = MqttNodeDesignId.Topic,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, value1Node, value2Node,],
            Host = "local",
        };

        var client = Substitute.For<IVirtualMqttClient>();
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        var message = new MqttApplicationMessageBuilder()
            .WithTopic("group")
            .WithPayload("""
            {
                "value1": 23
            }
            """)
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes($$"""
            {
                "value1":"{{typeof(int).AssemblyQualifiedName}}"
            }
            """))
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        messages.Should().ContainSingle().Which.Channel.Should().Be("gv1");
    }

    [Theory]
    [MemberData(nameof(GetSingleNodeVariants))]
    [SuppressMessage("Usage", "xUnit1044:Avoid using TheoryData type arguments that are not serializable",
        Justification = "Daten sind zu komplex und gehöre zur öffentlichen API")]
    [SuppressMessage("Usage", "xUnit1045:Avoid using TheoryData type arguments that might not be serializable",
        Justification = "Daten sind zu komplex und gehöre zur öffentlichen API")]
    public async Task Can_receive_single_node_Async(Node[] nodes, MqttApplicationMessage message, Action<List<ExternalValue>> assert)
    {
        MqttDataPortCommunication communication = new()
        {
            Host = "local",
            Nodes = nodes,
        };

        var client = Substitute.For<IVirtualMqttClient>();
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, new FakeTimeProvider(new(2023, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        assert(messages);
    }

    public static TheoryData<Node[], MqttApplicationMessage, Action<List<ExternalValue>>> GetSingleNodeVariants()
    {
        Node nodeWithoutType = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            ParentId = null,
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node nodeWithType = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            ParentId = null,
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(int),
        };
        Node nodeWithPlainTextSerializer = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            ParentId = null,
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(string),
            Properties = new()
            {
                { MqttNodeProperties.Serializer, new() { Value = (byte)2, } },
            }
        };
        Node nodeWithInheritedSerializer = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            ParentId = null,
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(string),
            Properties = new()
            {
                { MqttNodeProperties.Serializer, new() { Value = (byte)0, } },
            }
        };

        return new()
        {
            {
                new[] { nodeWithoutType },
                new MqttApplicationMessageBuilder()
                    .WithTopic("value")
                    .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(int).AssemblyQualifiedName ?? string.Empty))
                    .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc).ToString("O")))
                    .WithPayload(Encoding.UTF8.GetBytes("23"))
                    .Build(),
                m =>
                {
                    var value = m.Should().ContainSingle().Which;
                    value.Channel.Should().Be("v");
                    value.Value.Should().Be(23);
                    value.Timestamp.Should().Be(new(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc));
                }
            },
            {
                new[] { nodeWithoutType },
                new MqttApplicationMessageBuilder()
                    .WithTopic("value")
                    .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(int).AssemblyQualifiedName ?? string.Empty))
                    .WithUserProperty(MqttUserProperties.Validity, Encoding.UTF8.GetBytes("112"))
                    .Build(),
                m => m.Should().ContainSingle().Which.Validity.Should().Be(112)
            },
            {
                new[] { nodeWithoutType },
                new MqttApplicationMessageBuilder()
                    .WithTopic("value")
                    .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(int).AssemblyQualifiedName ?? string.Empty))
                    .Build(),
                m =>
                {
                    var value = m.Should().ContainSingle().Subject;
                    value.Validity.Should().Be(1);
                    value.Timestamp.Should().Be(new(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc));
                }
            },
            {
                new[] { nodeWithType },
                new MqttApplicationMessageBuilder()
                    .WithTopic("value")
                    .WithUserProperty(MqttUserProperties.Timestamp, new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc))
                    .WithPayload("23")
                    .Build(),
                m =>
                {
                    var value = m.Should().ContainSingle().Which;
                    value.Channel.Should().Be("v");
                    value.Value.Should().Be(23);
                    value.Timestamp.Should().Be(new(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc));
                }
            },
            {
                new[] { nodeWithPlainTextSerializer },
                new MqttApplicationMessageBuilder()
                    .WithTopic("value")
                    .WithUserProperty(MqttUserProperties.Timestamp, new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc))
                    .WithPayload("Test")
                    .Build(),
                m =>
                {
                    var value = m.Should().ContainSingle().Which;
                    value.Channel.Should().Be("v");
                    value.Value.Should().Be("Test");
                    value.Timestamp.Should().Be(new(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc));
                }
            },
            {
                new[] { nodeWithInheritedSerializer },
                new MqttApplicationMessageBuilder()
                    .WithTopic("value")
                    .WithUserProperty(MqttUserProperties.Timestamp, new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc))
                    .WithPayload("\"Test\"")
                    .Build(),
                m =>
                {
                    var value = m.Should().ContainSingle().Which;
                    value.Channel.Should().Be("v");
                    value.Value.Should().Be("Test");
                    value.Timestamp.Should().Be(new(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc));
                }
            },
        };
    }

    [Fact]
    public async Task Skips_a_message_for_an_unknown_topic_Async()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(int),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
            Host = "local",
        };

        var client = Substitute.For<IVirtualMqttClient>();
        FakeLogger<MqttDataPortIncoming> logger = new();
        using MqttDataPortIncoming incoming = new(communication, client, logger, AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        List<IReadOnlyCollection<ExternalValue>> batches = [];
        incoming.Received += batches.Add;

        var message = new MqttApplicationMessageBuilder()
            .WithTopic("somewhere/else")
            .WithPayload("23")
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        batches.Should().BeEmpty("a message that reaches no data point must not reach the engine as an empty batch");
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("somewhere/else"));
    }

    [Fact]
    public async Task Skips_a_data_point_that_transfers_none_of_its_channels_Async()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "unrelated", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(int),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
            Host = "local",
        };

        var client = Substitute.For<IVirtualMqttClient>();
        FakeLogger<MqttDataPortIncoming> logger = new();
        using MqttDataPortIncoming incoming = new(communication, client, logger, AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        var message = new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        messages.Should().BeEmpty();
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("value"));
    }

    [Fact]
    public async Task Can_handle_receive_failure_Async()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            ParentId = null,
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
        FakeLogger<MqttDataPortIncoming> logger = new();
        using MqttDataPortIncoming incoming = new(communication, client, logger, AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        incoming.Received += _ => throw new InvalidOperationException();

        var message = new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload($$"""
            {
                "Type": "{{typeof(int).AssemblyQualifiedName}}",
                "Value": 23
            }
            """)
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        logger.Collector.Count.Should().Be(1);
        logger.LatestRecord.Exception.Should().BeOfType<InvalidOperationException>();
        logger.LatestRecord.Message.Should().MatchEquivalentOf("*fail*receive*local*23*");
    }
}
