using System;
using System.Text;
using AwesomeAssertions;
using Microsoft.Extensions.Time.Testing;
using MQTTnet;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class ReceivedMqttMessage_Timestamp
{
    /// <summary>
    /// The timestamp reaches the engine and is written back onto the next outgoing message through
    /// <see cref="MqttEnvelopeCodec.FormatTimestamp"/>, which reads a timestamp of an unspecified
    /// kind as local time. Anything but UTC here moves every timestamp the port forwards by the
    /// offset of the host it runs on.
    /// </summary>
    [Theory]
    [InlineData("2026-03-04T12:00:00.0000000Z")]
    [InlineData("2026-03-04T14:00:00.0000000+02:00")]
    [InlineData("2026-03-04T12:00:00.0000000")]
    public void Reads_every_written_form_as_the_same_point_in_time_in_utc(string written)
    {
        ReceivedMqttMessage received = new(Message(written), TimeProvider.System);

        received.Timestamp.Should().Be(new(2026, 3, 4, 12, 0, 0, DateTimeKind.Utc));
        received.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public void Writes_back_the_timestamp_it_read()
    {
        ReceivedMqttMessage received = new(Message("2026-03-04T12:00:00.0000000Z"), TimeProvider.System);

        MqttEnvelopeCodec.FormatTimestamp(received.Timestamp).Should().Be("2026-03-04T12:00:00.0000000Z");
    }

    [Fact]
    public void Falls_back_to_the_receive_time_in_utc()
    {
        FakeTimeProvider timeProvider = new(new DateTimeOffset(2026, 3, 4, 12, 0, 0, TimeSpan.Zero));

        ReceivedMqttMessage received = new(new MqttApplicationMessageBuilder().WithTopic("temperature").Build(), timeProvider);

        received.Timestamp.Should().Be(new(2026, 3, 4, 12, 0, 0, DateTimeKind.Utc));
        received.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
    }

    private static MqttApplicationMessage Message(string timestamp)
        => new MqttApplicationMessageBuilder()
            .WithTopic("temperature")
            .WithUserProperty(MqttUserProperties.Timestamp, Encoding.UTF8.GetBytes(timestamp))
            .Build();
}
