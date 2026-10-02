using System;
using System.Collections.Generic;
using System.Runtime.Loader;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
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

public class MqttDataPortOutgoing_ctor
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
    public void Refuses_to_start_with_an_envelope_configuration_it_cannot_serve()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", "b", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node childNode = new()
        {
            Id = Guid.NewGuid(),
            ParentId = valueNode.Id,
            Name = "batchId",
            AffectedChannels = { "b", },
            DesignId = MqttNodeDesignId.UserProperty,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode, childNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V311,
        };

        var act = () => new MqttDataPortOutgoing(communication, Substitute.For<IVirtualMqttClient>(), Substitute.For<ILogger<MqttDataPortOutgoing>>());

        act.Should().Throw<InvalidOperationException>().WithMessage("*MQTT 3.1.1 cannot carry*");
    }

    [Fact]
    public void Refuses_to_start_with_an_unknown_quality_of_service_override()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            Properties = new()
            {
                { MqttNodeProperties.QualityOfServiceOverride, new() { Value = (byte)7, } },
            },
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
        };

        var act = () => new MqttDataPortOutgoing(communication, Substitute.For<IVirtualMqttClient>(), Substitute.For<ILogger<MqttDataPortOutgoing>>());

        act.Should().Throw<NotSupportedException>().WithMessage("*quality of service override '7'*");
    }

    /// <summary>
    /// A data point of a folder published as one group message has no message of its own, so a
    /// child under it could never be published. Such a data point transfers no value of its own —
    /// its folder does — which is the shape the port already refuses.
    /// </summary>
    [Fact]
    public void Refuses_an_envelope_child_under_a_data_point_of_a_folder_group()
    {
        Node folderNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "group",
            TransferredChannels = { "g", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            ParentId = folderNode.Id,
            Name = "value",
            AffectedChannels = { "g", },
            TransferredChannels = { "b", },
            DesignId = MqttNodeDesignId.Topic,
        };
        Node childNode = new()
        {
            Id = Guid.NewGuid(),
            ParentId = valueNode.Id,
            Name = "batchId",
            AffectedChannels = { "b", },
            DesignId = MqttNodeDesignId.UserProperty,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [folderNode, valueNode, childNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        var act = () => new MqttDataPortOutgoing(communication, Substitute.For<IVirtualMqttClient>(), Substitute.For<ILogger<MqttDataPortOutgoing>>());

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'value' has envelope children but transfers no value of its own*");
    }
}

public class MqttDataPortOutgoing_SendAsync
{
    private const string ParentChannel = "t";

    [Fact]
    public async Task Publishes_the_payload_content_type_and_format_Async()
    {
        var communication = CreateTree();
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(7, [Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);

        var message = messages.Should().ContainSingle().Which;
        message.ContentType.Should().Be("application/json");
        message.PayloadFormatIndicator.Should().Be(MqttPayloadFormatIndicator.CharacterData);
    }

    [Fact]
    public async Task Publishes_the_fixed_children_from_the_parent_value_Async()
    {
        var communication = CreateTree(
            (MqttNodeDesignId.Timestamp, "when", "c1"),
            (MqttNodeDesignId.Validity, "good", "c2"),
            (MqttNodeDesignId.UserProperty, "batchId", "c3"));
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(
            4711,
            [
                Value("c3", "B-7"),
                Value(ParentChannel, 21.5, new(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc)),
            ],
            TestContext.Current.CancellationToken);

        var message = messages.Should().ContainSingle().Which;
        UserProperties(message).Should().Equal(new Dictionary<string, string>
        {
            ["Timestamp"] = "2026-03-04T05:06:07.0000000Z",
            ["Validity"] = "100",
            ["batchId"] = "B-7",
        });
    }

    /// <summary>
    /// <c>Timestamp</c> is linkable inbound only, so a value that reaches the child outbound all
    /// the same is ignored: the message carries the timestamp of the parent value.
    /// </summary>
    [Fact]
    public async Task Publishes_the_timestamp_of_the_parent_value_and_not_one_written_to_the_child_Async()
    {
        var communication = CreateTree((MqttNodeDesignId.Timestamp, "when", "c1"));
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(
            1,
            [
                Value("c1", new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc)),
                Value(ParentChannel, 21.5, new(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc)),
            ],
            TestContext.Current.CancellationToken);

        UserProperties(messages.Should().ContainSingle().Which)["Timestamp"].Should().Be("2026-03-04T05:06:07.0000000Z");
    }

    [Fact]
    public async Task Publishes_the_value_type_when_the_parent_has_a_type_child_Async()
    {
        var communication = CreateTypeChildTree();
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(1, [Value(ParentChannel, 112),], TestContext.Current.CancellationToken);

        UserProperties(messages.Should().ContainSingle().Which).Should().Equal(new Dictionary<string, string>
        {
            [MqttUserProperties.Type] = typeof(int).AssemblyQualifiedName!,
        });
    }

    [Fact]
    public async Task Omits_the_value_type_without_a_type_child_Async()
    {
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(1, [Value(ParentChannel, 112),], TestContext.Current.CancellationToken);

        UserProperties(messages.Should().ContainSingle().Which).Should().BeEmpty();
    }

    private static MqttDataPortCommunication CreateTypeChildTree()
    {
        Node parentNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "temperature",
            DesignId = MqttNodeDesignId.Topic,
            AffectedChannels = { ParentChannel, },
            TransferredChannels = { ParentChannel, },
        };
        Node typeNode = new()
        {
            Id = Guid.NewGuid(),
            ParentId = parentNode.Id,
            Name = "type",
            DesignId = MqttNodeDesignId.Type,
        };

        return CreateCommunication([parentNode, typeNode,]);
    }

    [Fact]
    public async Task Publishes_one_message_for_a_parent_and_its_child_in_the_same_batch_Async()
    {
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(1, [Value("c1", "B-7"), Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);

        UserProperties(messages.Should().ContainSingle().Which).Should().Equal(new Dictionary<string, string> { ["batchId"] = "B-7", });
    }

    /// <summary>
    /// The one key a message leaves out, and the only one: a child the engine has not written has
    /// no value to put on the wire. That is the absence of a value, not metadata about one — the
    /// port never leaves a key out because of the state a value was written in.
    /// </summary>
    [Fact]
    public async Task Omits_a_child_no_value_was_ever_written_for_Async()
    {
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(1, [Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);

        UserProperties(messages.Should().ContainSingle().Which).Should().BeEmpty();
    }

    [Fact]
    public async Task Carries_a_child_value_from_an_earlier_batch_and_publishes_nothing_for_a_child_alone_Async()
    {
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(1, [Value("c1", "B-7"),], TestContext.Current.CancellationToken);
        messages.Should().BeEmpty();

        await outgoing.SendAsync(2, [Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);
        UserProperties(messages.Should().ContainSingle().Which).Should().Equal(new Dictionary<string, string> { ["batchId"] = "B-7", });

        await outgoing.SendAsync(3, [Value("c1", "B-8"),], TestContext.Current.CancellationToken);
        messages.Should().ContainSingle();
    }

    [Fact]
    public async Task Starts_a_new_port_instance_without_the_child_values_of_the_disposed_one_Async()
    {
        var communication = CreateTreeWithOneChild();
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using (MqttDataPortOutgoing disposed = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>()))
        {
            await disposed.SendAsync(1, [Value("c1", "B-7"),], TestContext.Current.CancellationToken);
        }

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        await outgoing.SendAsync(2, [Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);

        UserProperties(messages.Should().ContainSingle().Which).Should().BeEmpty();
    }

    /// <summary>
    /// A <c>User property</c> carries no validity of its own on the wire, so the port does not read
    /// one back as a rule about whether to publish: holding an invalid value back would signal it
    /// by omission, through a channel the receiver cannot tell from a key never written and the
    /// tree does not describe.
    /// </summary>
    [Fact]
    public async Task Publishes_the_last_child_value_the_engine_wrote_whatever_its_validity_Async()
    {
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(1, [Value("c1", "B-7", validity: 0), Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);
        UserProperties(messages[0]).Should().Equal(new Dictionary<string, string> { ["batchId"] = "B-7", });

        await outgoing.SendAsync(2, [Value("c1", "B-8"), Value(ParentChannel, 22.5),], TestContext.Current.CancellationToken);
        UserProperties(messages[1]).Should().Equal(new Dictionary<string, string> { ["batchId"] = "B-8", });
    }

    /// <summary>
    /// The keys a message carries come from the tree, never from the state of a value, so the same
    /// tree publishes the same envelope whether the engine calls its child valid or not.
    /// </summary>
    [Fact]
    public async Task Publishes_the_same_keys_for_a_valid_and_an_invalid_child_value_Async()
    {
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(1, [Value("c1", "B-7"), Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);
        await outgoing.SendAsync(2, [Value("c1", "B-8", validity: 0), Value(ParentChannel, 22.5),], TestContext.Current.CancellationToken);

        UserProperties(messages[1]).Keys.Should().Equal(UserProperties(messages[0]).Keys);
    }

    /// <summary>
    /// <see cref="MqttTextCodec.Format"/> writes a missing value as an empty text, and that is
    /// what a child written as null puts on the wire. The key still travels: the tree declares it,
    /// and null is a value the engine wrote rather than metadata about one.
    /// </summary>
    [Fact]
    public async Task Publishes_a_child_written_as_null_with_an_empty_text_Async()
    {
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        await outgoing.SendAsync(1, [Value("c1", null), Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);

        UserProperties(messages.Should().ContainSingle().Which).Should().Equal(new Dictionary<string, string> { ["batchId"] = "", });
    }

    [Fact]
    public async Task Publishes_the_cycles_in_the_order_they_arrived_Async()
    {
        TaskCompletionSource gate = new();
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        client.Publish(Arg.Any<MqttApplicationMessage>()).Returns(call =>
        {
            messages.Add(call.Arg<MqttApplicationMessage>());
            return messages.Count == 1 ? gate.Task : Task.CompletedTask;
        });
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        var firstCycle = outgoing.SendAsync(1, [Value("c1", "B-7"), Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);
        var secondCycle = outgoing.SendAsync(2, [Value("c1", "B-8"), Value(ParentChannel, 22.5),], TestContext.Current.CancellationToken);

        messages.Should().ContainSingle("the second cycle must not be processed while the first one is still publishing");

        gate.SetResult();
        await Task.WhenAll(firstCycle, secondCycle);

        messages.Should().HaveCount(2);
        SingleUserProperty(messages[0]).Should().Be("B-7");
        SingleUserProperty(messages[1]).Should().Be("B-8");
    }

    /// <summary>
    /// The client takes no cancellation token of its own, so a publish that never answers held its
    /// cycle, and with it every cycle chained behind it, for the life of the port.
    /// </summary>
    [Fact]
    public async Task Gives_up_a_publish_that_never_answers_when_its_cycle_is_cancelled_Async()
    {
        using CancellationTokenSource cancellation = new();
        TaskCompletionSource neverAnswers = new();
        List<MqttApplicationMessage> messages = [];
        var client = Substitute.For<IVirtualMqttClient>();
        client.Publish(Arg.Any<MqttApplicationMessage>()).Returns(call =>
        {
            messages.Add(call.Arg<MqttApplicationMessage>());
            return messages.Count == 1 ? neverAnswers.Task : Task.CompletedTask;
        });
        var communication = CreateTreeWithOneChild();
        communication.Host = "local";
        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        var stuckCycle = outgoing.SendAsync(1, [Value(ParentChannel, 21.5),], cancellation.Token);
        var nextCycle = outgoing.SendAsync(2, [Value(ParentChannel, 22.5),], TestContext.Current.CancellationToken);

        stuckCycle.IsCompleted.Should().BeFalse();

        await cancellation.CancelAsync();
        await stuckCycle.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await nextCycle.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        messages.Should().HaveCount(2, "a cancelled cycle must not hold up the one chained behind it");
    }

    /// <summary>
    /// Cancelling a cycle is how the engine stops the port, so it must not reach the log as a
    /// failure to send. Nothing was cancellable before the token was honoured at the publish.
    /// </summary>
    [Fact]
    public async Task Does_not_report_a_cancelled_cycle_as_a_failure_Async()
    {
        using CancellationTokenSource cancellation = new();
        TaskCompletionSource neverAnswers = new();
        var client = Substitute.For<IVirtualMqttClient>();
        client.Publish(Arg.Any<MqttApplicationMessage>()).Returns(neverAnswers.Task);
        var communication = CreateTreeWithOneChild();
        communication.Host = "local";
        FakeLogger<MqttDataPortOutgoing> logger = new();
        using MqttDataPortOutgoing outgoing = new(communication, client, logger);

        var cycle = outgoing.SendAsync(1, [Value(ParentChannel, 21.5),], cancellation.Token);
        await cancellation.CancelAsync();
        await cycle.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        logger.Collector.GetSnapshot().Should().NotContain(e => e.Level >= LogLevel.Error);
    }

    [Fact]
    public async Task Carries_a_child_of_one_cycle_on_the_parent_of_the_next_Async()
    {
        List<MqttApplicationMessage> messages = [];
        using var client = CreateRecordingClient(messages);
        using MqttDataPortOutgoing outgoing = new(CreateTreeWithOneChild(), client, Substitute.For<ILogger<MqttDataPortOutgoing>>());

        var childCycle = outgoing.SendAsync(1, [Value("c1", "B-7"),], TestContext.Current.CancellationToken);
        var parentCycle = outgoing.SendAsync(2, [Value(ParentChannel, 21.5),], TestContext.Current.CancellationToken);
        await Task.WhenAll(childCycle, parentCycle);

        SingleUserProperty(messages.Should().ContainSingle().Which).Should().Be("B-7");
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
        Encoding.UTF8.GetString(message.UserProperties.Should().Contain(p => p.Name == MqttUserProperties.Type).Which.ValueBuffer.Span).Should().Be(JsonSerializer.Serialize(JsonDocument.Parse($$""""
        {
            "value1": "{{typeof(int).AssemblyQualifiedName}}",
            "subgroup": {
                "value2": "{{typeof(int).AssemblyQualifiedName}}",
                "value3": "{{typeof(object).AssemblyQualifiedName}}"
            }
        }
        """")));
        message.UserProperties.Should().Contain(p => p.Name == MqttUserProperties.Timestamp).Which.GetDateTime().Should().Be(new(2023, 1, 1, 0, 10, 0, DateTimeKind.Utc));
        message.UserProperties.Should().Contain(p => p.Name == MqttUserProperties.Validity).Which.Get<int>().Should().Be(25);
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
    [InlineData(0, 21.5, "21.5")]
    [InlineData(1, 21.5, "21.5")]
    [InlineData(2, 21.5, "21.5")]
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

    [Theory]
    [InlineData(0, MqttQualityOfServiceLevel.AtLeastOnce)]
    [InlineData(1, MqttQualityOfServiceLevel.AtMostOnce)]
    [InlineData(2, MqttQualityOfServiceLevel.AtLeastOnce)]
    [InlineData(3, MqttQualityOfServiceLevel.ExactlyOnce)]
    public async Task Can_send_single_node_with_quality_of_service_override_Async(byte qualityOfServiceOverride, MqttQualityOfServiceLevel qualityOfService)
    {
        Node valueNode = new()
        {
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            Properties = new()
            {
                { MqttNodeProperties.QualityOfServiceOverride, new() { Value = qualityOfServiceOverride, } },
            },
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
            Host = "local",
            QualityOfService = (byte)MqttQualityOfServiceLevel.AtLeastOnce,
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

        messages.Should()
            .ContainSingle()
            .Which.QualityOfServiceLevel.Should().Be(qualityOfService);
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

        var message = messages.Should().ContainSingle().Which;
        message.ConvertPayloadToString().Should().Be("{\"myValue\":{\"Test\":112}}");
        message.UserProperties.Should().BeNullOrEmpty();
    }

    [Fact]
    public async Task Can_send_group_node_with_type_Async()
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
                Value = new JsonObject() { { "Test", 112 }, },
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        var json = Encoding.UTF8.GetString(messages.Should().ContainSingle().Which.UserProperties.Find(p => p.Name == MqttUserProperties.Type)!.ValueBuffer.Span);
        JsonNode.Parse(json)!["myValue"]!.GetValue<string>().Should().Be(typeof(JsonObject).AssemblyQualifiedName!);
    }

    internal static MqttDataPortCommunication CreateTreeWithOneChild()
        => CreateTree((MqttNodeDesignId.UserProperty, "batchId", "c1"));

    private static MqttDataPortCommunication CreateTree(params (string DesignId, string Name, string Channel)[] children)
    {
        Node parentNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "temperature",
            DesignId = MqttNodeDesignId.Topic,
            AffectedChannels = { ParentChannel, },
            TransferredChannels = { ParentChannel, },
        };
        List<Node> nodes = [parentNode,];

        foreach (var (designId, name, channel) in children)
        {
            parentNode.TransferredChannels.Add(channel);
            nodes.Add(new()
            {
                Id = Guid.NewGuid(),
                ParentId = parentNode.Id,
                Name = name,
                DesignId = designId,
                AffectedChannels = { channel, },
            });
        }

        return CreateCommunication([.. nodes,]);
    }

    private static MqttDataPortCommunication CreateCommunication(Node[] nodes)
    {
        MqttDataPortCommunication communication = new()
        {
            Nodes = nodes,
        };
        _ = new MqttDataPortProperties(communication)
        {
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        return communication;
    }

    internal static IVirtualMqttClient CreateRecordingClient(List<MqttApplicationMessage> messages)
    {
        var client = Substitute.For<IVirtualMqttClient>();
        client.When(c => c.Publish(Arg.Any<MqttApplicationMessage>())).Do(call => messages.Add(call.Arg<MqttApplicationMessage>()));

        return client;
    }

    internal static ExternalValue Value(string channel, object? value, DateTime timestamp = default, int validity = 100)
        => new()
        {
            Channel = channel,
            Value = value,
            Validity = validity,
            Timestamp = timestamp,
        };

    private static string SingleUserProperty(MqttApplicationMessage message)
        => Encoding.UTF8.GetString(message.UserProperties.Should().ContainSingle().Which.ValueBuffer.Span);

    private static Dictionary<string, string> UserProperties(MqttApplicationMessage message)
    {
        Dictionary<string, string> properties = [];

        foreach (var property in message.UserProperties ?? [])
            properties.Add(property.Name, Encoding.UTF8.GetString(property.ValueBuffer.Span));

        return properties;
    }
}
