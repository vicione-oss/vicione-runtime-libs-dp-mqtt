using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Loader;
using System.Text.Json;
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
    private readonly Serializer _defaultSerializer;
    private readonly IVirtualMqttClient _client;
    private readonly ILogger<MqttDataPortIncoming> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly MqttQualityOfServiceLevel _qualityOfService;
    private readonly Dictionary<Guid, IReadOnlyCollection<INode[]>> _transferredNodeJsonNodes;
    private readonly Dictionary<string, IReadOnlyCollection<INode>> _addressNodes;

    public event Action<IReadOnlyCollection<ExternalValue>>? Received;

    public MqttDataPortIncoming(MqttDataPortCommunication communication, ILoggerFactory loggerFactory, ILogger<MqttDataPortIncoming> logger, AssemblyLoadContext loadContext)
        : this(communication, communication.CreateClient(loggerFactory), logger, loadContext, TimeProvider.System)
    { }

    public MqttDataPortIncoming(MqttDataPortCommunication communication, IVirtualMqttClient virtualMqttClient, ILogger<MqttDataPortIncoming> logger, AssemblyLoadContext loadContext, TimeProvider timeProvider)
    {
        _communication = communication;
        _client = virtualMqttClient;
        _logger = logger;
        _timeProvider = timeProvider;
        _jsonOptions = JsonSetup.CreatePreserveTypeOptions(loadContext);
        MqttDataPortProperties properties = new(communication);
        _qualityOfService = properties.QualityOfService;
        _defaultSerializer = properties.DefaultSerializer;

        (_transferredNodeJsonNodes, _addressNodes) = InitializeUnspecificTree.InitializeIncoming(communication.Nodes, GenerateTopic);
    }

    private Task HandleIncomingValueAsync(MQTTnet.MqttApplicationMessageReceivedEventArgs eventArgs)
    {
        try
        {
            Received?.Invoke(MqttMessageHandler.HandleMessage(eventArgs, _transferredNodeJsonNodes, _addressNodes, _timeProvider, _jsonOptions, _defaultSerializer));
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
