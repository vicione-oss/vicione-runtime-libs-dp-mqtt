using System;
using Microsoft.Extensions.Logging;

namespace ViciOne.Suite.DataPort;

internal static partial class MqttDataPortOutgoingLog
{
    [LoggerMessage(0, LogLevel.Error, "Failed to send entries to '{Host}'.")]
    internal static partial void LogSendFailed(this ILogger<MqttDataPortOutgoing> logger, string host, Exception exception);
}
