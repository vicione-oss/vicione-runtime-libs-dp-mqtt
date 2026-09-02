using System;
using Microsoft.Extensions.Logging;

namespace ViciOne.Suite.DataPort;

internal static partial class MqttDataPortIncomingLog
{
    [LoggerMessage(0, LogLevel.Error, "Failed to handle received message from '{Host}'.")]
    internal static partial void LogReceiveFailed(this ILogger<MqttDataPortIncoming> logger, string host, Exception exception);

    [LoggerMessage(1, LogLevel.Warning, "Received a message for '{Topic}', which no data point of this data port addresses.")]
    internal static partial void LogUnknownTopic(this ILogger<MqttDataPortIncoming> logger, string topic);

    [LoggerMessage(2, LogLevel.Warning, "Cannot assign the value received on '{Topic}' to '{DataPoint}', which transfers none of its channels.")]
    internal static partial void LogChannelNotFound(this ILogger<MqttDataPortIncoming> logger, string topic, string dataPoint);

    [LoggerMessage(3, LogLevel.Warning, "Cannot read the payload received on '{Topic}' as '{ValueType}'. The data point is published as invalid.")]
    internal static partial void LogPayloadNotReadable(this ILogger<MqttDataPortIncoming> logger, string topic, string valueType, Exception? exception);

    [LoggerMessage(5, LogLevel.Warning, "Cannot read the envelope value '{Key}' of the message on '{Topic}'. The data point is published as invalid.")]
    internal static partial void LogEnvelopeValueNotReadable(this ILogger<MqttDataPortIncoming> logger, string key, string topic);
}
