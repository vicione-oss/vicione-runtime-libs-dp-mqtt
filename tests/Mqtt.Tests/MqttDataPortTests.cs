using System;
using System.Collections.Generic;
using System.Runtime.Loader;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
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

        messages.Should().HaveCount(2);
        messages.Should().BeEquivalentTo(values);
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
            ValueType = typeof(string),
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
        messages.Should().BeEquivalentTo(values);
    }
}
