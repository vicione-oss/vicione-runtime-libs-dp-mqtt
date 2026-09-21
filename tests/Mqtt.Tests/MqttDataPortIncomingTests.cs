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
using MQTTnet.Formatter;
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
            ValueType = typeof(int),
        };
        Node value2Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value2",
            ParentId = groupNode.Id,
            AffectedChannels = { "gv2", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(int),
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
            ValueType = typeof(int),
        };
        Node value2Node = new()
        {
            Id = Guid.NewGuid(),
            Name = "value2",
            ParentId = groupNode.Id,
            AffectedChannels = { "gv2", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(int),
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

    /// <summary>
    /// A member of a group message is read as the data type its data point is configured with, the
    /// same as a message of its own. The type a publisher declares is not consulted, so a topic
    /// cannot decide which type the port loads.
    /// </summary>
    [Fact]
    public async Task Ignores_the_type_a_publisher_declares_for_a_group_member_Async()
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
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes($$"""
            {
                "value":"{{typeof(long).AssemblyQualifiedName}}"
            }
            """))
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<int>().And.Be(23);
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
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V500,
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
                new[] { nodeWithType },
                new MqttApplicationMessageBuilder()
                    .WithTopic("value")
                    .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(string).AssemblyQualifiedName ?? string.Empty))
                    .WithUserProperty(MqttUserProperties.Validity, Encoding.UTF8.GetBytes("112"))
                    .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc).ToString("O")))
                    .WithPayload(Encoding.UTF8.GetBytes("23"))
                    .Build(),
                m =>
                {
                    var value = m.Should().ContainSingle().Which;
                    value.Channel.Should().Be("v");
                    value.Value.Should().Be(23);
                    value.Validity.Should().Be(112);
                    value.Timestamp.Should().Be(new(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc));
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
    public async Task Ignores_the_type_property_a_publisher_declared_Async()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(string),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
            Host = "local",
        };

        var messages = await ReceiveAsync(communication, new FakeLogger<MqttDataPortIncoming>(), new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("\"23\"")
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(int).AssemblyQualifiedName ?? string.Empty))
            .Build());

        var value = messages.Should().ContainSingle().Which;
        value.Value.Should().Be("23");
        value.Value.Should().BeOfType<string>();
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

    /// <summary>
    /// The port sends the engine validity, another sender may write a boolean instead, and a text
    /// that is neither leaves the value valid.
    /// </summary>
    [Theory]
    [InlineData("1", 1)]
    [InlineData("0", 0)]
    [InlineData("112", 112)]
    [InlineData("true", 1)]
    [InlineData("FALSE", 0)]
    [InlineData("maybe", 1)]
    public async Task Reads_the_validity_of_a_received_message_Async(string text, int expected)
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

        var messages = await ReceiveAsync(communication, new FakeLogger<MqttDataPortIncoming>(), new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Validity, Encoding.UTF8.GetBytes(text))
            .Build());

        messages.Should().ContainSingle().Which.Validity.Should().Be(expected);
    }

    /// <summary>
    /// A Timestamp that is not a round-trip formatted point in time no longer tears the message
    /// down; the value carries the time it was received instead.
    /// </summary>
    [Fact]
    public async Task Falls_back_to_the_receive_time_of_an_unreadable_timestamp_Async()
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

        var messages = await ReceiveAsync(communication, new FakeLogger<MqttDataPortIncoming>(), new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes("the day before"))
            .Build());

        messages.Should().ContainSingle().Which.Timestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(1));
    }

    [Fact]
    public async Task Publishes_the_data_point_and_its_envelope_children_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();

        var messages = await ReceiveAsync(communication, new FakeLogger<MqttDataPortIncoming>(), new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty("batchId", Encoding.UTF8.GetBytes("42"))
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(s_senderTimestamp.ToString("O")))
            .Build());

        messages.Should().SatisfyRespectively(
            parent =>
            {
                parent.Channel.Should().Be("v");
                parent.Value.Should().Be(23L);
                parent.Timestamp.Should().Be(s_senderTimestamp);
                parent.Validity.Should().Be(1);
            },
            batch =>
            {
                batch.Channel.Should().Be("batch");
                batch.Value.Should().Be(42L);
                batch.Timestamp.Should().Be(s_senderTimestamp);
                batch.Validity.Should().Be(1);
            },
            sent =>
            {
                sent.Channel.Should().Be("sent");
                sent.Value.Should().Be(s_senderTimestamp);
                sent.Timestamp.Should().Be(s_senderTimestamp);
                sent.Validity.Should().Be(1);
            });
    }

    [Fact]
    public async Task Publishes_an_envelope_child_without_a_property_as_invalid_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(s_senderTimestamp.ToString("O")))
            .Build());

        messages.Should().HaveCount(3);
        var batch = messages[1];
        batch.Channel.Should().Be("batch");
        batch.Validity.Should().Be(0);
        batch.Value.Should().Be(0L);
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    [Fact]
    public async Task Publishes_an_unparsable_envelope_child_as_invalid_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty("batchId", Encoding.UTF8.GetBytes("abc"))
            .Build());

        messages.Should().HaveCount(3);
        messages[0].Validity.Should().Be(1);
        messages[1].Channel.Should().Be("batch");
        messages[1].Validity.Should().Be(0);
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("batchId") && e.Message.Contains("value"));
    }

    [Fact]
    public async Task Publishes_the_envelope_children_of_an_unreadable_payload_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("not a number")
            .WithUserProperty("batchId", Encoding.UTF8.GetBytes("42"))
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(s_senderTimestamp.ToString("O")))
            .Build());

        messages.Should().HaveCount(3);
        messages[0].Validity.Should().Be(0);
        messages[0].Value.Should().Be(0L);
        messages[1].Value.Should().Be(42L);
        messages[2].Value.Should().Be(s_senderTimestamp);
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("value"));
    }

    /// <summary>
    /// A child is never more valid than the message that carried it, and it keeps the value it was
    /// written with: an engine reading only the child sees the reading and the flag the publisher
    /// put on it, rather than data the sender had already marked bad.
    /// </summary>
    [Fact]
    public async Task Publishes_the_envelope_children_of_an_invalid_message_as_invalid_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();

        var messages = await ReceiveAsync(communication, new FakeLogger<MqttDataPortIncoming>(), new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Validity, Encoding.UTF8.GetBytes("0"))
            .WithUserProperty("batchId", Encoding.UTF8.GetBytes("42"))
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(s_senderTimestamp.ToString("O")))
            .Build());

        messages.Should().SatisfyRespectively(
            parent =>
            {
                parent.Channel.Should().Be("v");
                parent.Value.Should().Be(23L);
                parent.Validity.Should().Be(0);
            },
            batch =>
            {
                batch.Channel.Should().Be("batch");
                batch.Value.Should().Be(42L);
                batch.Validity.Should().Be(0);
            },
            sent =>
            {
                sent.Channel.Should().Be("sent");
                sent.Value.Should().Be(s_senderTimestamp);
                sent.Validity.Should().Be(0);
            });
    }

    /// <summary>
    /// The engine validity travels as the integer it is, so a child of a valid message carries the
    /// state its publisher gave the value and not only that it was valid.
    /// </summary>
    [Fact]
    public async Task Publishes_the_envelope_children_with_the_engine_validity_of_the_message_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();

        var messages = await ReceiveAsync(communication, new FakeLogger<MqttDataPortIncoming>(), new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Validity, Encoding.UTF8.GetBytes("112"))
            .WithUserProperty("batchId", Encoding.UTF8.GetBytes("42"))
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(s_senderTimestamp.ToString("O")))
            .Build());

        messages.Should().HaveCount(3);
        messages.Should().AllSatisfy(value => value.Validity.Should().Be(112));
    }

    /// <summary>
    /// A fixed child the engine never linked has no channel of its own, and the tree is accepted
    /// all the same: a <c>Timestamp</c> is the one child the engine may leave unlinked and still
    /// see on the wire outbound.
    /// </summary>
    [Fact]
    public async Task Skips_an_envelope_child_the_engine_has_not_linked_Async()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(long),
        };
        Node sentNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "sent",
            ParentId = valueNode.Id,
            DesignId = MqttNodeDesignId.Timestamp,
            ValueType = typeof(DateTime),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode, sentNode,],
            Host = "local",
        };
        _ = new MqttDataPortProperties(communication) { ProtocolVersion = MqttProtocolVersion.V500, };

        var messages = await ReceiveAsync(communication, new FakeLogger<MqttDataPortIncoming>(), new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(s_senderTimestamp.ToString("O")))
            .Build());

        messages.Should().ContainSingle().Which.Channel.Should().Be("v");
    }

    private static readonly DateTime s_senderTimestamp = new(2024, 4, 1, 12, 0, 0, DateTimeKind.Utc);

    private static (MqttDataPortCommunication Communication, Node Parent) CreateTreeWithEnvelopeChildren()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", "batch", "sent", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(long),
        };
        Node batchNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "batchId",
            ParentId = valueNode.Id,
            AffectedChannels = { "batch", },
            DesignId = MqttNodeDesignId.UserProperty,
            ValueType = typeof(long),
        };
        Node sentNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "sent",
            ParentId = valueNode.Id,
            AffectedChannels = { "sent", },
            DesignId = MqttNodeDesignId.Timestamp,
            ValueType = typeof(DateTime),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode, batchNode, sentNode,],
            Host = "local",
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        return (communication, valueNode);
    }

    private static async Task<List<ExternalValue>> ReceiveAsync(MqttDataPortCommunication communication, ILogger<MqttDataPortIncoming> logger, MqttApplicationMessage message)
    {
        var client = Substitute.For<IVirtualMqttClient>();
        using MqttDataPortIncoming incoming = new(communication, client, logger, AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);
        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        return messages;
    }

    [Fact]
    public async Task Publishes_an_invalid_value_if_the_payload_cannot_be_read_Async()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(long),
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
            .WithPayload("not a number")
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        var value = messages.Should().ContainSingle().Which;
        value.Channel.Should().Be("v");
        value.Validity.Should().Be(0);
        value.Value.Should().Be(0L);
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
            ValueType = typeof(int),
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
            .WithPayload("23")
            .Build();

        client.MessageReceived += Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(new MqttApplicationMessageReceivedEventArgs(
            string.Empty, message, new(), (_1, _2) => Task.CompletedTask));

        logger.Collector.Count.Should().Be(1);
        logger.LatestRecord.Exception.Should().BeOfType<InvalidOperationException>();
        logger.LatestRecord.Message.Should().MatchEquivalentOf("*fail*receive*local*23*");
    }
}
