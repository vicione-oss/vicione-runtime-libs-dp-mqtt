using System;
using Microsoft.Extensions.Logging;
using MQTTnet.Protocol;

namespace ViciOne.Suite.DataPort;

internal static partial class MqttDataPortIncomingLog
{
    [LoggerMessage(0, LogLevel.Error, "Failed to handle received message from '{Host}'.")]
    internal static partial void LogReceiveFailed(this ILogger<MqttDataPortIncoming> logger, string host, Exception exception);

    [LoggerMessage(1, LogLevel.Warning, "Received a message for '{Topic}', which no data point of this data port addresses.")]
    internal static partial void LogUnknownTopic(this ILogger<MqttDataPortIncoming> logger, string topic);

    [LoggerMessage(2, LogLevel.Warning, "Cannot read the payload received on '{Topic}' as '{ValueType}'. Its value is not forwarded to the engine.")]
    internal static partial void LogPayloadNotReadable(this ILogger<MqttDataPortIncoming> logger, string topic, string valueType, Exception? exception);

    [LoggerMessage(3, LogLevel.Warning, "Cannot load the data type '{ValueType}' the message on '{Topic}' names. The value is read as the data type its data point declares.")]
    internal static partial void LogNamedValueTypeNotLoadable(this ILogger<MqttDataPortIncoming> logger, string valueType, string topic);

    [LoggerMessage(4, LogLevel.Warning, "Cannot read the envelope value '{Key}' of the message received on '{Topic}'. No value is forwarded for it.")]
    internal static partial void LogEnvelopeValueNotReadable(this ILogger<MqttDataPortIncoming> logger, string key, string topic);

    [LoggerMessage(5, LogLevel.Warning, "Cannot read '{Timestamp}' as the point in time the message on '{Topic}' was sent at. The time it was received is used instead.")]
    internal static partial void LogTimestampNotReadable(this ILogger<MqttDataPortIncoming> logger, string timestamp, string topic);

    [LoggerMessage(6, LogLevel.Warning, "Cannot read '{Validity}' as the validity of the message on '{Topic}'. Its values are forwarded as invalid.")]
    internal static partial void LogValidityNotReadable(this ILogger<MqttDataPortIncoming> logger, string validity, string topic);

    [LoggerMessage(7, LogLevel.Warning, "The data points of topic '{Topic}' request different qualities of service. Subscribing with the highest one, '{QualityOfService}'.")]
    internal static partial void LogAmbiguousQualityOfService(this ILogger<MqttDataPortIncoming> logger, string topic, MqttQualityOfServiceLevel qualityOfService);
}
