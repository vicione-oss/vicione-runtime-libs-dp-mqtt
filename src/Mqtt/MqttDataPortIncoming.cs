using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MQTTnet.Extensions;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using ViciOne.ManagedEngine.ExternalCommunication;

namespace ViciOne.Suite.DataPort;

public sealed class MqttDataPortIncoming : IExternalIncomingCommunication<MqttDataPortCommunication>, IDisposable
{
    private readonly MqttDataPortCommunication _communication;
    private readonly IVirtualMqttClient _client;
    private readonly ILogger<MqttDataPortIncoming> _logger;
    private readonly bool _cleanSession;
    private readonly Dictionary<string, (MqttQualityOfServiceLevel Level, bool IsAmbiguous)> _subscriptions;
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
        _cleanSession = properties.CleanSession ?? true;

        var envelopeChildren = EnvelopeChildren.Create(communication.Nodes, properties.ProtocolVersion == MqttProtocolVersion.V500);
        var (transferredNodeJsonNodes, addressNodes) = InitializeUnspecificTree.InitializeIncoming(communication.Nodes, GenerateTopic);
        _subscriptions = addressNodes.ToDictionary(a => a.Key, a => a.Value.ResolveQualityOfService(properties.QualityOfService));
        _messageHandler = new(transferredNodeJsonNodes, addressNodes, timeProvider, JsonSetup.CreatePayloadOptions(loadContext), loadContext, properties.DefaultSerializer, envelopeChildren, logger);
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
        foreach (var (topic, (qualityOfService, isAmbiguous)) in _subscriptions)
        {
            if (isAmbiguous)
                _logger.LogAmbiguousQualityOfService(topic, qualityOfService);

            await _client.Subscribe(topic, qualityOfService, false);
        }
    }

    /// <summary>
    /// A persistent session keeps its subscriptions on the broker, which queues the messages that
    /// arrive while this port is away, so only a clean session unsubscribes.
    /// </summary>
    public async Task DisconnectAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_cleanSession)
            {
                foreach (var topic in _subscriptions.Keys)
                    await _client.Unsubscribe(topic);
            }
        }
        finally
        {
            await _client.Disconnect();
            _client.MessageReceived -= HandleIncomingValueAsync;
        }
    }

    public void Dispose()
        => _client.Dispose();
}
