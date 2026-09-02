using System;
using System.Collections.Generic;
using System.Linq;
using AwesomeAssertions;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class EnvelopeChildren_Create
{
    [Fact]
    public void Maps_a_parent_to_its_children_and_every_child_channel_to_its_child()
    {
        var parent = Parent("temperature", "t");
        var batchId = Child(parent, MqttNodeDesignId.UserProperty, "batchId", "b");
        var validity = Child(parent, MqttNodeDesignId.Validity, "valid", "va");

        var children = EnvelopeChildren.Create([parent, batchId, validity,], supportsUserProperties: true);

        children.IsEmpty.Should().BeFalse();
        children.Of(parent.Id).Should().SatisfyRespectively(
            c => c.Should().BeEquivalentTo(new { Kind = EnvelopeChildKind.UserProperty, Key = "batchId", }),
            c => c.Should().BeEquivalentTo(new { Kind = EnvelopeChildKind.Validity, Key = "Validity", }));
        children.TryGetChild("b", out var child).Should().BeTrue();
        child.Key.Should().Be("batchId");
        children.TryGetChild("t", out _).Should().BeFalse();
    }

    [Fact]
    public void Accepts_a_tree_without_envelope_children()
    {
        var parent = Parent("temperature", "t");

        EnvelopeChildren.Create([parent,], supportsUserProperties: false).IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void Rejects_two_children_whose_keys_differ_only_in_case()
    {
        var parent = Parent("temperature", "t");

        var act = () => EnvelopeChildren.Create(
            [parent, Child(parent, MqttNodeDesignId.UserProperty, "batchId", "b1"), Child(parent, MqttNodeDesignId.UserProperty, "BATCHID", "b2"),],
            supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>().WithMessage("*more than one envelope child for the key 'BATCHID'*");
    }

    [Fact]
    public void Rejects_more_than_one_of_a_fixed_child()
    {
        var parent = Parent("temperature", "t");

        var act = () => EnvelopeChildren.Create(
            [parent, Child(parent, MqttNodeDesignId.Timestamp, "when", "w1"), Child(parent, MqttNodeDesignId.Timestamp, "when again", "w2"),],
            supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>().WithMessage("*more than one envelope child for the key 'Timestamp'*");
    }

    [Theory]
    [InlineData("Timestamp")]
    [InlineData("validity")]
    [InlineData("TYPE")]
    public void Rejects_a_user_property_that_shadows_a_reserved_key(string name)
    {
        var parent = Parent("temperature", "t");

        var act = () => EnvelopeChildren.Create(
            [parent, Child(parent, MqttNodeDesignId.UserProperty, name, "b"),],
            supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>().WithMessage($"*reserved envelope key '{name}'*");
    }

    [Fact]
    public void Rejects_envelope_children_on_a_protocol_that_cannot_carry_them()
    {
        var parent = Parent("temperature", "t");

        var act = () => EnvelopeChildren.Create(
            [parent, Child(parent, MqttNodeDesignId.UserProperty, "batchId", "b"),],
            supportsUserProperties: false);

        act.Should().Throw<InvalidOperationException>().WithMessage("*MQTT 3.1.1 cannot carry*");
    }

    [Fact]
    public void Rejects_a_child_of_a_child()
    {
        var parent = Parent("temperature", "t");
        var batchId = Child(parent, MqttNodeDesignId.UserProperty, "batchId", "b");

        var act = () => EnvelopeChildren.Create(
            [parent, batchId, Child(batchId, MqttNodeDesignId.UserProperty, "deeper", "d"),],
            supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>().WithMessage("*'batchId' is an envelope child and cannot have envelope children*");
    }

    [Fact]
    public void Rejects_a_parent_that_transfers_no_value_of_its_own()
    {
        Node parent = new()
        {
            Id = Guid.NewGuid(),
            Name = "temperature",
            DesignId = MqttNodeDesignId.Topic,
            TransferredChannels = { "b", },
        };

        var act = () => EnvelopeChildren.Create(
            [parent, Child(parent, MqttNodeDesignId.UserProperty, "batchId", "b"),],
            supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>().WithMessage("*transfers no value of its own*");
    }

    [Fact]
    public void Rejects_more_user_properties_than_a_message_may_carry()
    {
        var parent = Parent("temperature", "t");
        List<Node> nodes = [parent,];

        for (var i = 0; i <= EnvelopeChildren.MaxUserProperties; i++)
            nodes.Add(Child(parent, MqttNodeDesignId.UserProperty, $"key{i}", $"c{i}"));

        var act = () => EnvelopeChildren.Create(nodes, supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>().WithMessage("*has 17 user properties*");
    }

    [Fact]
    public void Accepts_a_type_child_without_a_channel()
    {
        var parent = Parent("temperature", "t");
        Node typeChild = new()
        {
            Id = Guid.NewGuid(),
            ParentId = parent.Id,
            Name = "type",
            DesignId = MqttNodeDesignId.Type,
        };

        var children = EnvelopeChildren.Create([parent, typeChild,], supportsUserProperties: true);

        children.Of(parent.Id).Should().ContainSingle().Which.Should().BeEquivalentTo(new { Kind = EnvelopeChildKind.Type, Key = "Type", ValueType = (Type?)null, });
    }

    [Fact]
    public void Rejects_more_than_one_type_child()
    {
        var parent = Parent("temperature", "t");
        Node first = new() { Id = Guid.NewGuid(), ParentId = parent.Id, Name = "type1", DesignId = MqttNodeDesignId.Type, };
        Node second = new() { Id = Guid.NewGuid(), ParentId = parent.Id, Name = "type2", DesignId = MqttNodeDesignId.Type, };

        var act = () => EnvelopeChildren.Create([parent, first, second,], supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>().WithMessage("*more than one envelope child for the key 'Type'*");
    }

    [Fact]
    public void Rejects_a_child_whose_channel_the_parent_does_not_transfer()
    {
        var parent = Parent("temperature", "t");
        Node batchId = new()
        {
            Id = Guid.NewGuid(),
            ParentId = parent.Id,
            Name = "batchId",
            DesignId = MqttNodeDesignId.UserProperty,
            AffectedChannels = { "b", },
        };

        var act = () => EnvelopeChildren.Create([parent, batchId,], supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'batchId' is an envelope child of 'temperature', which transfers none of its channels*");
    }

    /// <summary>
    /// The children the port derives from the parent value are never linked, so the engine gives
    /// them no channel; only a user property needs one.
    /// </summary>
    [Theory]
    [InlineData(MqttNodeDesignId.Timestamp)]
    [InlineData(MqttNodeDesignId.Validity)]
    [InlineData(MqttNodeDesignId.Type)]
    public void Accepts_a_fixed_child_the_engine_gave_no_channel(string designId)
    {
        var parent = Parent("temperature", "t");
        Node child = new() { Id = Guid.NewGuid(), ParentId = parent.Id, Name = "child", DesignId = designId, };

        var act = () => EnvelopeChildren.Create([parent, child,], supportsUserProperties: true);

        act.Should().NotThrow();
    }

    /// <summary>
    /// The ruleset offers all four fixed children under every data point and the engine links none
    /// of them outbound, so the configuration it invites carries four children without a channel.
    /// None of them can be looked up by one.
    /// </summary>
    [Fact]
    public void Accepts_every_fixed_child_the_engine_gave_no_channel()
    {
        var parent = Parent("temperature", "t");

        var children = EnvelopeChildren.Create(
            [
                parent,
                FixedChild(parent, MqttNodeDesignId.Timestamp, "when"),
                FixedChild(parent, MqttNodeDesignId.Validity, "valid"),
                FixedChild(parent, MqttNodeDesignId.Type, "type"),
            ],
            supportsUserProperties: true);

        children.Of(parent.Id).Select(c => c.Key).Should().Equal("Timestamp", "Validity", "Type");
        children.TryGetChild(string.Empty, out _).Should().BeFalse();
    }

    [Fact]
    public void Rejects_a_child_whose_parent_is_not_part_of_the_data_port()
    {
        var parent = Parent("temperature", "t");
        var batchId = Child(parent, MqttNodeDesignId.UserProperty, "batchId", "b");

        var act = () => EnvelopeChildren.Create([batchId,], supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'batchId' is an envelope child of a data point that is not part of this data port*");
    }

    [Fact]
    public void Rejects_two_children_that_resolve_to_the_same_channel()
    {
        var parent = Parent("temperature", "t");

        var act = () => EnvelopeChildren.Create(
            [parent, Child(parent, MqttNodeDesignId.UserProperty, "batchId", "b"), Child(parent, MqttNodeDesignId.UserProperty, "orderId", "b"),],
            supportsUserProperties: true);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'batchId' and 'orderId' are envelope children of the same value*");
    }

    [Fact]
    public void Reports_every_violation_at_once()
    {
        var parent = Parent("temperature", "t");

        var act = () => EnvelopeChildren.Create(
            [parent, Child(parent, MqttNodeDesignId.UserProperty, "Timestamp", "b1"), Child(parent, MqttNodeDesignId.UserProperty, "timestamp", "b2"),],
            supportsUserProperties: false);

        act.Should().Throw<InvalidOperationException>()
            .Which.Message.Should().Contain("MQTT 3.1.1 cannot carry").And.Contain("reserved envelope key 'Timestamp'").And.Contain("more than one envelope child");
    }

    private static Node Parent(string name, string channel)
        => new()
        {
            Id = Guid.NewGuid(),
            Name = name,
            DesignId = MqttNodeDesignId.Topic,
            AffectedChannels = { channel, },
            TransferredChannels = { channel, },
        };

    private static Node FixedChild(Node parent, string designId, string name)
        => new()
        {
            Id = Guid.NewGuid(),
            ParentId = parent.Id,
            Name = name,
            DesignId = designId,
        };

    private static Node Child(Node parent, string designId, string name, string channel)
    {
        parent.TransferredChannels.Add(channel);

        return new()
        {
            Id = Guid.NewGuid(),
            ParentId = parent.Id,
            Name = name,
            DesignId = designId,
            AffectedChannels = { channel, },
        };
    }
}
