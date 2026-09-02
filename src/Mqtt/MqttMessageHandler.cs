using System;
using System.Collections.Generic;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ViciOne.ManagedEngine.ExternalCommunication;

namespace ViciOne.Suite.DataPort;

internal sealed class MqttMessageHandler
{
    private readonly Dictionary<Guid, IReadOnlyCollection<INode[]>> _jsonRoutesByNode;
    private readonly Dictionary<string, IReadOnlyCollection<INode>> _nodesByTopic;
    private readonly TimeProvider _timeProvider;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly Serializer _defaultSerializer;
    private readonly ILogger<MqttDataPortIncoming> _logger;

    internal MqttMessageHandler(
        Dictionary<Guid, IReadOnlyCollection<INode[]>> jsonRoutesByNode,
        Dictionary<string, IReadOnlyCollection<INode>> nodesByTopic,
        TimeProvider timeProvider,
        JsonSerializerOptions jsonOptions,
        Serializer defaultSerializer,
        ILogger<MqttDataPortIncoming> logger)
    {
        _jsonRoutesByNode = jsonRoutesByNode;
        _nodesByTopic = nodesByTopic;
        _timeProvider = timeProvider;
        _jsonOptions = jsonOptions;
        _defaultSerializer = defaultSerializer;
        _logger = logger;
    }

    internal List<ExternalValue> HandleMessage(MQTTnet.MqttApplicationMessageReceivedEventArgs eventArgs)
    {
        var topic = eventArgs.ApplicationMessage.Topic;

        if (!_nodesByTopic.TryGetValue(topic, out var nodes))
        {
            _logger.LogUnknownTopic(topic);
            return [];
        }

        ReceivedMqttMessage received = new(eventArgs.ApplicationMessage, _timeProvider);

        foreach (var node in nodes)
            ProcessNode(received, node);

        return received.Values;
    }

    private void ProcessNode(ReceivedMqttMessage received, INode node)
    {
        if (_jsonRoutesByNode.TryGetValue(node.Id, out var routes))
            ProcessGroupValue(received, node, routes);
        else
            ProcessValue(received, node);
    }

    private void ProcessValue(ReceivedMqttMessage received, INode node)
    {
        var serializer = node.GetSerializer().ApplyDefault(_defaultSerializer);
        var valueType = received.TypeProperty?.GetAsType() ?? node.ValueType ?? throw new InvalidOperationException("Type of value not found.");

        AddValue(received, node, node.AffectedChannels, serializer.Deserialize(received.Message.Payload, valueType, _jsonOptions));
    }

    private void ProcessGroupValue(ReceivedMqttMessage received, INode node, IReadOnlyCollection<INode[]> routes)
    {
        var jsonMessage = received.Message.GetPayloadAsJsonNode();

        foreach (var route in routes)
        {
            var dataPointNode = route[^1];
            var valueJson = NodeValueFactory.GetValueJsonNode(route, jsonMessage);

            if (valueJson is null)
                continue;

            var valueType = NodeValueFactory.GetValueJsonNode(route, received.MetaJson)?.GetValue<string>().ToType() ?? dataPointNode.ValueType ?? throw new InvalidOperationException("Type of value not found.");

            AddValue(received, node, dataPointNode.AffectedChannels, valueJson.Deserialize(valueType, _jsonOptions));
        }
    }

    private void AddValue(ReceivedMqttMessage received, INode node, IReadOnlyCollection<string> affectedChannels, object? value)
    {
        if (!TryGetChannel(received, node, affectedChannels, out var channel))
            return;

        received.Values.Add(new()
        {
            Channel = channel,
            Value = value,
            Timestamp = received.Timestamp,
            Validity = received.Validity,
        });
    }

    private bool TryGetChannel(ReceivedMqttMessage received, INode node, IReadOnlyCollection<string> affectedChannels, out string channel)
    {
        foreach (var affectedChannel in affectedChannels)
        {
            if (node.TransferredChannels.Contains(affectedChannel))
            {
                channel = affectedChannel;
                return true;
            }
        }

        _logger.LogChannelNotFound(received.Message.Topic, node.Name);
        channel = string.Empty;
        return false;
    }
}
