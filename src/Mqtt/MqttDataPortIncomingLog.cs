using System;
using Microsoft.Extensions.Logging;

namespace ViciOne.Suite.DataPort;

internal static partial class MqttDataPortIncomingLog
{
    [LoggerMessage(0, LogLevel.Error, "Failed to handle received message from '{Host}'.")]
    internal static partial void LogReceiveFailed(this ILogger<MqttDataPortIncoming> logger, string host, Exception exception);

    [LoggerMessage(1, LogLevel.Warning, "Received a message for '{Topic}', which no data point of this data port addresses.")]
    internal static partial void LogUnknownTopic(this ILogger<MqttDataPortIncoming> logger, string topic);

    [LoggerMessage(2, LogLevel.Warning, "Cannot read the payload received on '{Topic}' as '{ValueType}'. Its value is not forwarded to the engine.")]
    internal static partial void LogPayloadNotReadable(this ILogger<MqttDataPortIncoming> logger, string topic, string valueType, Exception? exception);

    [LoggerMessage(3, LogLevel.Warning, "Cannot read the envelope value '{Key}' of the message received on '{Topic}'. No value is forwarded for it.")]
    internal static partial void LogEnvelopeValueNotReadable(this ILogger<MqttDataPortIncoming> logger, string key, string topic);
}
