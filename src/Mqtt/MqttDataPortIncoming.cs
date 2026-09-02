using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MQTTnet.Extensions;
using MQTTnet.Protocol;
using ViciOne.ManagedEngine.ExternalCommunication;

namespace ViciOne.Suite.DataPort;

public sealed class MqttDataPortIncoming : IExternalIncomingCommunication<MqttDataPortCommunication>, IDisposable
{
    private readonly MqttDataPortCommunication _communication;
    private readonly IVirtualMqttClient _client;
    private readonly ILogger<MqttDataPortIncoming> _logger;
    private readonly MqttQualityOfServiceLevel _qualityOfService;
    private readonly Dictionary<string, IReadOnlyCollection<INode>> _addressNodes;
    private readonly MqttMessageHandler _messageHandler;

    public event Action<IReadOnlyCollection<ExternalValue>>? Received;

    public MqttDataPortIncoming(MqttDataPortCommunication communication, ILoggerFactory loggerFactory, ILogger<MqttDataPortIncoming> logger, AssemblyLoadContext loadContext)
        : this(communication, communication.CreateClient(loggerFactory), logger, loadContext, TimeProvider.System)
    { }

    public MqttDataPortIncoming(MqttDataPortCommunication communication, IVirtualMqttClient virtualMqttClient, ILogger<MqttDataPortIncoming> logger, AssemblyLoadContext loadContext, TimeProvider timeProvider)
    {
        _communication = communication;
        _client = virtualMqttClient;
        _logger = logger;
        MqttDataPortProperties properties = new(communication);
        _qualityOfService = properties.QualityOfService;

        var (transferredNodeJsonNodes, addressNodes) = InitializeUnspecificTree.InitializeIncoming(communication.Nodes, GenerateTopic);
        _addressNodes = addressNodes;
        _messageHandler = new(transferredNodeJsonNodes, addressNodes, timeProvider, JsonSetup.CreatePreserveTypeOptions(loadContext), properties.DefaultSerializer, logger);
    }

    private Task HandleIncomingValueAsync(MQTTnet.MqttApplicationMessageReceivedEventArgs eventArgs)
    {
        try
        {
            var values = _messageHandler.HandleMessage(eventArgs);

            if (values.Count != 0)
                Received?.Invoke(values);
        }
        catch (Exception ex)
        {
            _logger.LogReceiveFailed(_communication.GetName(), ex);
        }

        return Task.CompletedTask;
    }

    private static string GenerateTopic(IEnumerable<INode> structure)
        => structure
            .Where(s => !string.IsNullOrWhiteSpace(s.Name))
            .Select(s => s.Name)
            .Aggregate((a, b) => a + "/" + b);

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        _client.MessageReceived += HandleIncomingValueAsync;

        await _client.Connect();
        foreach (var topic in _addressNodes.Keys)
            await _client.Subscribe(topic, _qualityOfService, false);
    }

    public async Task DisconnectAsync(CancellationToken cancellationToken)
    {
        await _client.Disconnect();
        foreach (var topic in _addressNodes.Keys)
            await _client.Unsubscribe(topic);

        _client.MessageReceived -= HandleIncomingValueAsync;
    }

    public void Dispose()
        => _client.Dispose();
}
