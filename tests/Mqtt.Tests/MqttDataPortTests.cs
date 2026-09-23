using System;
using System.Collections.Generic;
using System.Runtime.Loader;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using MQTTnet;
using MQTTnet.Extensions;
using MQTTnet.Formatter;
using NSubstitute;
using ViciOne.ManagedEngine.ExternalCommunication;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttDataPort_
{
    [Fact]
    public async Task Outgoing_is_compatible_with_incoming()
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
            ValueType = typeof(int),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, valueNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = string.Empty,
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        var client = Substitute.For<IVirtualMqttClient>();
        client.When(c => c.Publish(Arg.Any<MqttApplicationMessage>())).Do(callInfo => client.MessageReceived +=
            Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(
                new MqttApplicationMessageReceivedEventArgs(string.Empty, callInfo.Arg<MqttApplicationMessage>(), new(), (_1, _2) => Task.CompletedTask)));

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);

        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        var timestamp = DateTime.UtcNow;
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

        // The group message carries its envelope; the data point declares no children, so its own message carries none.
        messages.Should().HaveCount(2);
        messages.Should().ContainSingle(m => m.Channel == "gv").Which.Should().BeEquivalentTo(values[0]);
        var single = messages.Should().ContainSingle(m => m.Channel == "v").Which;
        single.Should().BeEquivalentTo(values[1], o => o.Excluding(v => v.Timestamp).Excluding(v => v.Validity));
        single.Validity.Should().Be(1);
    }

    [Fact]
    public async Task Outgoing_is_compatible_with_incoming_with_envelope_children_Async()
    {
        var communication = CreateEnvelopeTree();
        using var client = CreateLoopbackClient();

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);

        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        var timestamp = DateTime.UtcNow;
        List<ExternalValue> values =
        [
            new() { Channel = "batch", Value = 42L, Validity = 1, Timestamp = timestamp, },
            new() { Channel = "v", Value = 23L, Validity = 1, Timestamp = timestamp, },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().SatisfyRespectively(
            parent =>
            {
                parent.Channel.Should().Be("v");
                parent.Value.Should().Be(23L);
            },
            batch =>
            {
                batch.Channel.Should().Be("batch");
                batch.Value.Should().Be(42L);
            },
            sent =>
            {
                sent.Channel.Should().Be("sent");
                sent.Value.Should().Be(timestamp);
            });
        messages.Should().AllSatisfy(m => m.Validity.Should().Be(1));
        messages.Should().AllSatisfy(m => m.Timestamp.Should().Be(timestamp));
    }

    /// <summary>
    /// A child written as invalid still travels — its validity has no place on the wire, so it
    /// cannot decide the keys — and the receiver gives it the validity of the message it arrived
    /// on. The state of the child is what is lost in the round trip; its reading is not.
    /// </summary>
    [Fact]
    public async Task Outgoing_is_compatible_with_incoming_with_an_invalid_envelope_child_Async()
    {
        var communication = CreateEnvelopeTree();
        using var client = CreateLoopbackClient();

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);

        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        var timestamp = DateTime.UtcNow;
        List<ExternalValue> values =
        [
            new() { Channel = "batch", Value = 42L, Validity = 0, Timestamp = timestamp, },
            new() { Channel = "v", Value = 23L, Validity = 1, Timestamp = timestamp, },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().SatisfyRespectively(
            parent => parent.Channel.Should().Be("v"),
            batch =>
            {
                batch.Channel.Should().Be("batch");
                batch.Value.Should().Be(42L);
            },
            sent => sent.Channel.Should().Be("sent"));
        messages.Should().AllSatisfy(m => m.Validity.Should().Be(1));
    }

    /// <summary>
    /// The <c>Type</c> child carries the type of the published value, and the receiving data point
    /// reads the payload as it, bounded by the type its own tree declares. A value published as the
    /// declared type arrives as that type, unchanged by the round trip.
    /// </summary>
    [Fact]
    public async Task Outgoing_is_compatible_with_incoming_with_a_type_child_Async()
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
        Node typeNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "type",
            ParentId = valueNode.Id,
            DesignId = MqttNodeDesignId.Type,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode, typeNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = string.Empty,
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        var client = Substitute.For<IVirtualMqttClient>();
        client.When(c => c.Publish(Arg.Any<MqttApplicationMessage>())).Do(callInfo => client.MessageReceived +=
            Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(
                new MqttApplicationMessageReceivedEventArgs(string.Empty, callInfo.Arg<MqttApplicationMessage>(), new(), (_1, _2) => Task.CompletedTask)));

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);

        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        List<ExternalValue> values = [new() { Channel = "v", Value = 112L, Validity = 100, Timestamp = DateTime.UtcNow, },];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        var value = messages.Should().ContainSingle().Which;
        value.Channel.Should().Be("v");
        value.Value.Should().Be(112L);
        value.Value.Should().BeOfType<long>();
    }

    /// <summary>
    /// The engine may deliver a narrower primitive than the tree declares, and the outgoing side
    /// names the runtime type it published — an <c>Int32</c> for an <c>Int64</c> data point. That is
    /// ordinary traffic between two of these ports: the value arrives as the declared type and
    /// nothing is logged about it.
    /// </summary>
    [Fact]
    public async Task Outgoing_is_compatible_with_incoming_for_a_narrower_primitive_Async()
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
        Node typeNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "type",
            ParentId = valueNode.Id,
            DesignId = MqttNodeDesignId.Type,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode, typeNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = string.Empty,
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        using var client = CreateLoopbackClient();
        FakeLogger<MqttDataPortIncoming> logger = new();

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        using MqttDataPortIncoming incoming = new(communication, client, logger, AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);

        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        List<ExternalValue> values = [new() { Channel = "v", Value = 23, Validity = 1, Timestamp = DateTime.UtcNow, },];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().ContainSingle().Which.Value.Should().Be(23L);
        logger.Collector.GetSnapshot().Should().BeEmpty();
    }

    /// <summary>
    /// What the <c>Type</c> child is for: a value published as a type derived from the one the tree
    /// declares arrives with the members the declared type does not know still on it. Without the
    /// child the payload is read as <see cref="Measurement"/> and <c>Unit</c> is lost, because the
    /// payload itself carries no type of its own.
    /// </summary>
    [Fact]
    public async Task Outgoing_is_compatible_with_incoming_for_a_derived_value_Async()
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
        Node typeNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "type",
            ParentId = valueNode.Id,
            DesignId = MqttNodeDesignId.Type,
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode, typeNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = string.Empty,
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        using var client = CreateLoopbackClient();

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);

        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        DetailedMeasurement published = new() { Value = 23, Unit = "bar", };
        List<ExternalValue> values = [new() { Channel = "v", Value = published, Validity = 1, Timestamp = DateTime.UtcNow, },];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<DetailedMeasurement>().And.Be(published);
    }

    /// <summary>
    /// The same for a member of a group message, whose type travels in the <c>Type</c> of the group
    /// message rather than in a child of its own.
    /// </summary>
    [Fact]
    public async Task Outgoing_is_compatible_with_incoming_for_a_derived_group_member_Async()
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
            ValueType = typeof(Measurement),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [groupNode, valueNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = string.Empty,
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        using var client = CreateLoopbackClient();

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);

        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        DetailedMeasurement published = new() { Value = 23, Unit = "bar", };
        List<ExternalValue> values = [new() { Channel = "gv", Value = published, Validity = 1, Timestamp = DateTime.UtcNow, },];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().ContainSingle().Which.Value.Should().BeOfType<DetailedMeasurement>().And.Be(published);
    }

    [Theory]
    [InlineData(0, "Test")]
    [InlineData(1, "Test")]
    [InlineData(2, "Test")]
    [InlineData(0, 20.2)]
    [InlineData(1, 20.2)]
    [InlineData(2, 20.2)]
    public async Task Outgoing_is_compatible_with_incoming_with_serializer(byte serializer, object? value)
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", "gv", },
            TransferredChannels = { "v", },
            DesignId = MqttNodeDesignId.Topic,
            ValueType = value!.GetType(),
            Properties = new()
            {
                { MqttNodeProperties.Serializer, new() { Value = serializer, } },
            }
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = string.Empty,
            ProtocolVersion = MqttProtocolVersion.V500,
        };
        var client = Substitute.For<IVirtualMqttClient>();
        client.When(c => c.Publish(Arg.Any<MqttApplicationMessage>())).Do(callInfo => client.MessageReceived +=
            Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(
                new MqttApplicationMessageReceivedEventArgs(string.Empty, callInfo.Arg<MqttApplicationMessage>(), new(), (_1, _2) => Task.CompletedTask)));

        using MqttDataPortOutgoing outgoing = new(communication, client, Substitute.For<ILogger<MqttDataPortOutgoing>>());
        using MqttDataPortIncoming incoming = new(communication, client, Substitute.For<ILogger<MqttDataPortIncoming>>(), AssemblyLoadContext.Default, TimeProvider.System);
        await incoming.ConnectAsync(TestContext.Current.CancellationToken);

        List<ExternalValue> messages = [];
        incoming.Received += messages.AddRange;

        var timestamp = DateTime.UtcNow;
        List<ExternalValue> values =
        [
            new()
            {
                Channel = "v",
                Value = value,
                Validity = 100,
                Timestamp = timestamp,
            },
        ];

        await outgoing.SendAsync(0, values, TestContext.Current.CancellationToken);

        messages.Should().ContainSingle();
        messages.Should().BeEquivalentTo(values, o => o.Excluding(v => v.Timestamp).Excluding(v => v.Validity));
        messages.Should().AllSatisfy(m => m.Validity.Should().Be(1));
    }

    /// <summary>
    /// One data point with a child of every kind the engine can link: a <c>User property</c>, a
    /// <c>Timestamp</c> and a <c>Validity</c>.
    /// </summary>
    private static MqttDataPortCommunication CreateEnvelopeTree()
    {
        Node valueNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "value",
            AffectedChannels = { "v", },
            TransferredChannels = { "v", "batch", "sent", "valid", },
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
        Node validNode = new()
        {
            Id = Guid.NewGuid(),
            Name = "valid",
            ParentId = valueNode.Id,
            AffectedChannels = { "valid", },
            DesignId = MqttNodeDesignId.Validity,
            ValueType = typeof(long),
        };
        MqttDataPortCommunication communication = new()
        {
            Nodes = [valueNode, batchNode, sentNode, validNode,],
        };
        _ = new MqttDataPortProperties(communication)
        {
            Protocol = MqttProtocol.Tcp,
            Host = string.Empty,
            ProtocolVersion = MqttProtocolVersion.V500,
        };

        return communication;
    }

    /// <summary>
    /// A client that hands every published message straight back to its own subscribers, so the
    /// outgoing port writes the envelope the incoming port then reads.
    /// </summary>
    private static IVirtualMqttClient CreateLoopbackClient()
    {
        var client = Substitute.For<IVirtualMqttClient>();

        client.When(c => c.Publish(Arg.Any<MqttApplicationMessage>())).Do(callInfo => client.MessageReceived +=
            Raise.Event<Func<MqttApplicationMessageReceivedEventArgs, Task>>(
                new MqttApplicationMessageReceivedEventArgs(string.Empty, callInfo.Arg<MqttApplicationMessage>(), new(), (_1, _2) => Task.CompletedTask)));

        return client;
    }
}
