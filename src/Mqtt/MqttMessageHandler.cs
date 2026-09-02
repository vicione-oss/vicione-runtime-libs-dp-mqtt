using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using ViciOne.ManagedEngine.ExternalCommunication;

namespace ViciOne.Suite.DataPort;

internal sealed class MqttMessageHandler
{
    private readonly Dictionary<Guid, IReadOnlyCollection<INode[]>> _jsonRoutesByNode;
    private readonly Dictionary<string, IReadOnlyCollection<INode>> _nodesByTopic;
    private readonly TimeProvider _timeProvider;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly Serializer _defaultSerializer;

    internal MqttMessageHandler(
        Dictionary<Guid, IReadOnlyCollection<INode[]>> jsonRoutesByNode,
        Dictionary<string, IReadOnlyCollection<INode>> nodesByTopic,
        TimeProvider timeProvider,
        JsonSerializerOptions jsonOptions,
        Serializer defaultSerializer)
    {
        _jsonRoutesByNode = jsonRoutesByNode;
        _nodesByTopic = nodesByTopic;
        _timeProvider = timeProvider;
        _jsonOptions = jsonOptions;
        _defaultSerializer = defaultSerializer;
    }

    internal List<ExternalValue> HandleMessage(MQTTnet.MqttApplicationMessageReceivedEventArgs eventArgs)
    {
        ReceivedMqttMessage received = new(eventArgs.ApplicationMessage, _timeProvider);
        var nodes = _nodesByTopic[received.Message.Topic];

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

    private static void AddValue(ReceivedMqttMessage received, INode node, IReadOnlyCollection<string> affectedChannels, object? value)
        => received.Values.Add(new()
        {
            Channel = affectedChannels.First(node.TransferredChannels.Contains),
            Value = value,
            Timestamp = received.Timestamp,
            Validity = received.Validity,
        });
}
