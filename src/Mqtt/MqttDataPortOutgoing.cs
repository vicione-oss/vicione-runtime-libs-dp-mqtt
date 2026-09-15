using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MQTTnet.Extensions;
using ViciOne.ManagedEngine.ExternalCommunication;

namespace ViciOne.Suite.DataPort;

public sealed class MqttDataPortOutgoing : IExternalOutgoingCommunication<MqttDataPortCommunication>, IDisposable
{
    private readonly MqttDataPortCommunication _communication;
    private readonly IVirtualMqttClient _client;
    private readonly ILogger<MqttDataPortOutgoing> _logger;
    private readonly MqttTreeHandlerOutgoing _mqttTreeHandlerOutgoing;

    public MqttDataPortOutgoing(MqttDataPortCommunication communication, ILoggerFactory loggerFactory, ILogger<MqttDataPortOutgoing> logger)
        : this(communication, communication.CreateClient(loggerFactory), logger)
    { }

    public MqttDataPortOutgoing(MqttDataPortCommunication communication, IVirtualMqttClient virtualMqttClient, ILogger<MqttDataPortOutgoing> logger)
    {
        _communication = communication;
        _client = virtualMqttClient;
        _logger = logger;
        _mqttTreeHandlerOutgoing = new(communication, _client);

        _mqttTreeHandlerOutgoing.Initialize();
    }


    public async Task ConnectAsync(CancellationToken cancellationToken)
        => await _client.Connect();

    public async Task DisconnectAsync(CancellationToken cancellationToken)
        => await _client.Disconnect();

    public void Dispose()
        => _client.Dispose();

    public async Task SendAsync(ulong engineCycle, IReadOnlyCollection<ExternalValue> values, CancellationToken cancellationToken)
    {
        try
        {
            await _mqttTreeHandlerOutgoing.Send(values, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The engine cancels a cycle to stop the port, which is not a failure to send.
        }
        catch (Exception ex)
        {
            _logger.LogSendFailed(_communication.GetName(), ex);
        }
    }
}
