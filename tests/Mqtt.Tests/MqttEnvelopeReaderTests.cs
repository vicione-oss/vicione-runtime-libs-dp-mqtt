using System;
using System.Text;
using AwesomeAssertions;
using MQTTnet;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class MqttEnvelopeReader_TryRead
{
    [Fact]
    public void Reads_a_user_property_whose_key_matches_exactly()
    {
        var result = MqttEnvelopeReader.TryRead(UserProperty("batchId", typeof(string)), Message(("batchId", "B-7")), out var value);

        result.Should().Be(EnvelopeReadResult.Read);
        value.Should().Be("B-7");
    }

    [Fact]
    public void Ignores_a_user_property_whose_key_differs_in_case()
    {
        var result = MqttEnvelopeReader.TryRead(UserProperty("batchId", typeof(string)), Message(("BATCHID", "B-7")), out var value);

        result.Should().Be(EnvelopeReadResult.Missing);
        value.Should().BeNull();
    }

    [Theory]
    [InlineData("Timestamp")]
    [InlineData("TIMESTAMP")]
    [InlineData("timestamp")]
    public void Reads_a_reserved_key_regardless_of_case(string key)
    {
        var result = MqttEnvelopeReader.TryRead(Timestamp(), Message((key, "2026-03-04T05:06:07.0000000Z")), out var value);

        result.Should().Be(EnvelopeReadResult.Read);
        value.Should().Be(new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc));
    }

    [Fact]
    public void Reads_the_first_property_of_a_key_the_message_carries_twice()
    {
        var result = MqttEnvelopeReader.TryRead(UserProperty("batchId", typeof(string)), Message(("batchId", "first"), ("batchId", "second")), out var value);

        result.Should().Be(EnvelopeReadResult.Read);
        value.Should().Be("first");
    }

    [Fact]
    public void Reads_a_property_the_message_does_not_carry_as_missing()
    {
        var result = MqttEnvelopeReader.TryRead(UserProperty("batchId", typeof(string)), Message(("orderId", "B-7")), out var value);

        result.Should().Be(EnvelopeReadResult.Missing);
        value.Should().BeNull();
    }

    [Fact]
    public void Reads_a_text_the_declared_type_cannot_read_as_malformed()
    {
        var result = MqttEnvelopeReader.TryRead(UserProperty("batchId", typeof(long)), Message(("batchId", "B-7")), out var value);

        result.Should().Be(EnvelopeReadResult.Malformed);
        value.Should().BeNull();
    }

    [Fact]
    public void Falls_back_to_the_data_type_of_its_kind_when_the_node_declares_none()
    {
        var child = Timestamp();

        var result = MqttEnvelopeReader.TryRead(child, Message((child.Key, "2026-03-04T05:06:07.0000000Z")), out var value);

        result.Should().Be(EnvelopeReadResult.Read);
        value.Should().BeOfType(child.ValueType);
    }

    [Fact]
    public void Reads_a_timestamp_that_is_not_a_round_trip_timestamp_as_malformed()
    {
        var result = MqttEnvelopeReader.TryRead(Timestamp(), Message((MqttUserProperties.Timestamp, "yesterday")), out _);

        result.Should().Be(EnvelopeReadResult.Malformed);
    }

    private static EnvelopeChild UserProperty(string name, Type valueType)
        => EnvelopeChild.Create(
            new Node { Id = Guid.NewGuid(), Name = name, DesignId = MqttNodeDesignId.UserProperty, ValueType = valueType, },
            EnvelopeChildKind.UserProperty,
            "c");

    /// <summary>
    /// The only fixed child a received message is read for: the others travel outbound only.
    /// </summary>
    private static EnvelopeChild Timestamp()
        => EnvelopeChild.Create(
            new Node { Id = Guid.NewGuid(), Name = "child", DesignId = MqttNodeDesignId.Timestamp, },
            EnvelopeChildKind.Timestamp,
            "c");

    private static MqttApplicationMessage Message(params (string Key, string Value)[] properties)
    {
        var builder = new MqttApplicationMessageBuilder().WithTopic("value");

        foreach (var (key, value) in properties)
            builder.WithUserProperty(key, Encoding.UTF8.GetBytes(value));

        return builder.Build();
    }
}
