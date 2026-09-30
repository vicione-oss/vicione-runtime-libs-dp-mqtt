using Microsoft.Extensions.Logging;

namespace ViciOne.Suite.DataPort;

internal static partial class MqttSetupLog
{
    [LoggerMessage(0, LogLevel.Warning, "TLS is configured, but port {Port} is the port of plain MQTT. A broker that accepts no TLS there refuses the connection; select 'No TLS' or the TLS port of the broker.")]
    internal static partial void LogTlsOnPlaintextPort(this ILogger logger, int port);

    [LoggerMessage(1, LogLevel.Warning, "'No TLS' is configured, but port {Port} is the port of MQTT over TLS. A broker that expects TLS there refuses the connection; select a TLS mode or the plain MQTT port of the broker.")]
    internal static partial void LogPlaintextOnTlsPort(this ILogger logger, int port);
}
