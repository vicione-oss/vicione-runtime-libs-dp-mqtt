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

public class MqttDataPortIncoming_ctor
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
}

public class MqttDataPortIncoming_ConnectAsync
{
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
}

public class MqttDataPortIncoming_HandleIncomingValueAsync
{
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
    /// A member of a group message is read like a message of its own: the name its entry in the
    /// <c>Type</c> of the message gives is used only when it is a subtype of the declared data type,
    /// so a topic cannot decide which type the port loads.
    /// </summary>
    [Fact]
    public async Task Reads_a_group_member_as_its_declared_type_when_the_named_one_is_no_subtype_Async()
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

    [Fact]
    public async Task Reads_a_group_member_as_the_derived_type_its_sender_names_Async()
    {
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveGroupOfOneAsync(typeof(Measurement), logger, new MqttApplicationMessageBuilder()
            .WithPayload("""{"value":{"Value":23,"Unit":"bar"}}""")
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes($$"""{"value":"{{typeof(DetailedMeasurement).AssemblyQualifiedName}}"}""")));

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<DetailedMeasurement>().Which.Unit.Should().Be("bar");
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    [Theory]
    [InlineData("System.Int64")]
    [InlineData("[]")]
    [InlineData("{")]
    [InlineData("""{"value":42}""")]
    public async Task Reads_a_group_member_as_its_declared_type_when_the_type_names_none_for_it_Async(string type)
    {
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveGroupOfOneAsync(typeof(int), logger, new MqttApplicationMessageBuilder()
            .WithPayload("""{"value":23}""")
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(type)));

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<int>().And.Be(23);
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    private static Task<List<ExternalValue>> ReceiveGroupOfOneAsync(Type valueType, FakeLogger<MqttDataPortIncoming> logger, MqttApplicationMessageBuilder message)
    {
        Node groupNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "group",
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
            ValueType = valueType,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, valueNode,],
            Host = "local",
        };

        return ReceiveAsync(communication, logger, message.WithTopic("group").Build());
    }

    /// <summary>
    /// A member the configured data type cannot read is left out rather than forwarded with the
    /// default of that type, which would be indistinguishable from a reading the sender took. The
    /// members around it still reach the engine.
    /// </summary>
    [Fact]
    public async Task Forwards_only_the_members_of_a_group_message_that_can_be_read_Async()
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
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("group")
            .WithPayload("""
            {
                "value1": "not a number",
                "value2": 23
            }
            """)
            .Build());

        var value = messages.Should().ContainSingle().Which;
        value.Channel.Should().Be("gv2");
        value.Value.Should().Be(23);
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("group") && e.Message.Contains("Int32"));
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

    /// <summary>
    /// The data type its data point declares is the contract, so a sender that names one the
    /// declared type cannot hold does not get to decide which type this port loads.
    /// </summary>
    [Fact]
    public async Task Ignores_a_type_the_declared_one_cannot_hold_Async()
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
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("\"23\"")
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(int).AssemblyQualifiedName ?? string.Empty))
            .Build());

        var value = messages.Should().ContainSingle().Which;
        value.Value.Should().Be("23");
        value.Value.Should().BeOfType<string>();
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    /// <summary>
    /// The outgoing side names the runtime type of the value it published, which is
    /// <see cref="object"/> for a null and may be a narrower primitive than the tree declares. Both
    /// reach a data point whose declared type cannot hold them, and neither is worth a word: the
    /// declared type is used and nothing is logged.
    /// </summary>
    [Theory]
    [InlineData("System.Object")]
    [InlineData("System.Int32")]
    public async Task Reads_a_payload_as_its_declared_type_when_the_named_one_is_no_subtype_Async(string typeName)
    {
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveMeasurementAsync(
            new MqttApplicationMessageBuilder()
                .WithTopic("value")
                .WithPayload("""{"Value":23,"Unit":"bar"}""")
                .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(Type.GetType(typeName)!.AssemblyQualifiedName!)),
            logger);

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<Measurement>();
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    /// <summary>
    /// What carries a derived type across a link of two of these ports: the sender names the type it
    /// published, and the receiver reads the payload as that type instead of losing the members the
    /// declared one does not know.
    /// </summary>
    [Fact]
    public async Task Reads_a_payload_as_the_derived_type_its_sender_names_Async()
    {
        var messages = await ReceiveMeasurementAsync(
            new MqttApplicationMessageBuilder()
                .WithTopic("value")
                .WithPayload("""{"Value":23,"Unit":"bar"}""")
                .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(DetailedMeasurement).AssemblyQualifiedName!)),
            new FakeLogger<MqttDataPortIncoming>());

        var value = messages.Should().ContainSingle().Which.Value;
        value.Should().BeOfType<DetailedMeasurement>().Which.Unit.Should().Be("bar");
    }

    [Fact]
    public async Task Reads_a_payload_as_its_declared_type_when_the_message_names_none_Async()
    {
        var messages = await ReceiveMeasurementAsync(
            new MqttApplicationMessageBuilder()
                .WithTopic("value")
                .WithPayload("""{"Value":23,"Unit":"bar"}"""),
            new FakeLogger<MqttDataPortIncoming>());

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<Measurement>();
    }

    [Fact]
    public async Task Ignores_a_type_no_assembly_of_this_port_knows_Async()
    {
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveMeasurementAsync(
            new MqttApplicationMessageBuilder()
                .WithTopic("value")
                .WithPayload("""{"Value":23,"Unit":"bar"}""")
                .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes("Nowhere.NoSuchType, Nowhere")),
            logger);

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<Measurement>();
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("NoSuchType") && e.Message.Contains("value"));
    }

    /// <summary>
    /// The name always comes from a value that existed, so an abstract type is one no port wrote.
    /// Reading the payload as it would drop the value the declared type could have read.
    /// </summary>
    [Fact]
    public async Task Ignores_an_abstract_type_a_message_names_Async()
    {
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveMeasurementAsync(
            new MqttApplicationMessageBuilder()
                .WithTopic("value")
                .WithPayload("""{"Value":23}""")
                .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(PartialMeasurement).AssemblyQualifiedName!)),
            logger);

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<Measurement>();
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    /// <summary>
    /// The shape this is for once a data point may declare a complex data type: the declared type is
    /// a base no value can have been, and only the name on the wire says which of its subtypes the
    /// payload is.
    /// </summary>
    [Fact]
    public async Task Reads_a_payload_as_the_subtype_its_sender_names_for_an_abstract_data_type_Async()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(PartialMeasurement),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
            Host = "local",
        };
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("""{"Value":23,"Unit":"bar"}""")
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(CompletedMeasurement).AssemblyQualifiedName!))
            .Build());

        var value = messages.Should().ContainSingle().Which.Value;
        value.Should().BeOfType<CompletedMeasurement>().Which.Unit.Should().Be("bar");
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    private static Task<List<ExternalValue>> ReceiveMeasurementAsync(MqttApplicationMessageBuilder message, FakeLogger<MqttDataPortIncoming> logger)
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = typeof(Measurement),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
            Host = "local",
        };

        return ReceiveAsync(communication, logger, message.Build());
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

        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes("the day before"))
            .Build());

        messages.Should().ContainSingle().Which.Timestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(1));
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("the day before") && e.Message.Contains("value"));
    }

    /// <summary>
    /// A `Timestamp` child reads the same property the message was read from, so one unreadable text
    /// is one warning and not two, and the child that could not be read forwards nothing.
    /// </summary>
    [Fact]
    public async Task Reports_an_unreadable_timestamp_once_for_a_message_and_its_child_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty("batchId", Encoding.UTF8.GetBytes("42"))
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes("the day before"))
            .Build());

        messages.Should().SatisfyRespectively(
            parent => parent.Channel.Should().Be("v"),
            batch => batch.Channel.Should().Be("batch"));
        logger.Collector.GetSnapshot().Should().ContainSingle().Which.Message
            .Should().Contain("the day before");
    }

    /// <summary>
    /// A message that names no time at all is only missing one, so the receive time stands in for it
    /// without a word. Only a time the sender did name and this port could not read is worth one.
    /// </summary>
    [Fact]
    public async Task Falls_back_to_the_receive_time_of_a_message_without_a_timestamp_Async()
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
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .Build());

        messages.Should().ContainSingle().Which.Timestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(1));
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    /// <summary>
    /// The point of reading more than the round-trip format: a publisher that is not this port
    /// writes plain ISO 8601, and the time it names reaches the engine instead of the receive time.
    /// </summary>
    [Fact]
    public async Task Reads_a_timestamp_a_foreign_publisher_wrote_as_plain_iso_8601_Async()
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
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes("2026-03-04T05:06:07Z"))
            .Build());

        messages.Should().ContainSingle().Which.Timestamp.Should().Be(new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc));
        logger.Collector.GetSnapshot().Should().BeEmpty();
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

    /// <summary>
    /// The engine keeps whatever it had on that channel. A key the message leaves out is not an
    /// event, so it is not logged either.
    /// </summary>
    [Fact]
    public async Task Forwards_no_value_for_an_envelope_child_without_a_property_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(s_senderTimestamp.ToString("O")))
            .Build());

        messages.Should().SatisfyRespectively(
            parent => parent.Channel.Should().Be("v"),
            sent => sent.Channel.Should().Be("sent"));
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    /// <summary>
    /// Unlike a missing key, a text that is not a value of the child's data type is worth a warning:
    /// the sender meant to say something the port could not read.
    /// </summary>
    [Fact]
    public async Task Forwards_no_value_for_an_unparsable_envelope_child_Async()
    {
        var (communication, _) = CreateTreeWithEnvelopeChildren();
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty("batchId", Encoding.UTF8.GetBytes("abc"))
            .Build());

        var parent = messages.Should().ContainSingle().Which;
        parent.Channel.Should().Be("v");
        parent.Validity.Should().Be(1);
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

        messages.Should().HaveCount(2);
        messages[0].Channel.Should().Be("batch");
        messages[0].Value.Should().Be(42L);
        messages[1].Value.Should().Be(s_senderTimestamp);
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
    public async Task Forwards_no_value_if_the_payload_cannot_be_read_Async()
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

        messages.Should().BeEmpty();
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("value"));
    }

    /// <summary>
    /// A data point the tree gives no data type has nothing to read its payload with. The type a
    /// publisher declares is not consulted for it either, so there is no value to forward.
    /// </summary>
    [Fact]
    public async Task Forwards_no_value_if_the_data_point_declares_no_data_type_Async()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
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
        FakeLogger<MqttDataPortIncoming> logger = new();

        var messages = await ReceiveAsync(communication, logger, new MqttApplicationMessageBuilder()
            .WithTopic("value")
            .WithPayload("23")
            .WithUserProperty(MqttUserProperties.Type, Encoding.UTF8.GetBytes(typeof(long).AssemblyQualifiedName!))
            .Build());

        messages.Should().BeEmpty();
        logger.Collector.GetSnapshot().Should().ContainSingle(e =>
            e.Level == LogLevel.Warning && e.Message.Contains("value") && e.Message.Contains("unknown"));
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
