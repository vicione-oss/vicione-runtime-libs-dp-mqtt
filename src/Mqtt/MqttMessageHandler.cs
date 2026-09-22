using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
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
    private readonly EnvelopeChildren _envelopeChildren;
    private readonly ILogger<MqttDataPortIncoming> _logger;

    internal MqttMessageHandler(
        Dictionary<Guid, IReadOnlyCollection<INode[]>> jsonRoutesByNode,
        Dictionary<string, IReadOnlyCollection<INode>> nodesByTopic,
        TimeProvider timeProvider,
        JsonSerializerOptions jsonOptions,
        Serializer defaultSerializer,
        EnvelopeChildren envelopeChildren,
        ILogger<MqttDataPortIncoming> logger)
    {
        _jsonRoutesByNode = jsonRoutesByNode;
        _nodesByTopic = nodesByTopic;
        _timeProvider = timeProvider;
        _jsonOptions = jsonOptions;
        _defaultSerializer = defaultSerializer;
        _envelopeChildren = envelopeChildren;
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

    /// <summary>
    /// Forwards the value of <paramref name="node"/> followed by its envelope children. A payload
    /// the configured data type cannot read is forwarded as no value at all rather than as a made-up
    /// one: it names a sender the port does not speak the same language as, and the default of the
    /// data type would be indistinguishable from a reading the sender actually took. The envelope
    /// children are read either way, as each of them carries its own text.
    /// </summary>
    private void ProcessValue(ReceivedMqttMessage received, INode node)
    {
        if (TryDeserializePayload(received, node, node.ValueType, out var value))
            AddValue(received, node, node.AffectedChannels, value);

        AddEnvelopeChildren(received, node);
    }

    /// <summary>
    /// The value an envelope child that could not be read is forwarded with. A typed incoming link
    /// casts the received value to its own type without a null check, so a null would fail the
    /// transfer of a value type instead of arriving as invalid.
    /// </summary>
    private static object? InvalidValueOf(Type? type)
        => type is { IsValueType: true } ? Activator.CreateInstance(type) : null;

    /// <summary>
    /// Publishes one value per envelope child of <paramref name="node"/> that fans out to a value of
    /// its own, in the order the tree declares them, so a received message reaches the engine as the
    /// data point and its children together. <see cref="EnvelopeChildKind.Validity"/> and
    /// <see cref="EnvelopeChildKind.Type"/> are outbound only, so neither is a fan-out target; the
    /// validity of the received message reaches the engine as the validity of the data point and of
    /// every child read from it, so a child is never more valid than the message that carried it. A
    /// message that declares no validity is valid, as it has always been.
    /// </summary>
    private void AddEnvelopeChildren(ReceivedMqttMessage received, INode node)
    {
        var children = _envelopeChildren.Of(node.Id);

        for (var i = 0; i < children.Count; i++)
        {
            var child = children[i];

            if (child.Kind is EnvelopeChildKind.Validity or EnvelopeChildKind.Type)
                continue;

            // A Timestamp the engine reads outbound but never linked inbound has no channel of its
            // own, and the engine routes nothing by the empty channel every one of them would share.
            if (child.Channel.Length == 0)
                continue;

            AddEnvelopeChild(received, child);
        }
    }

    private void AddEnvelopeChild(ReceivedMqttMessage received, EnvelopeChild child)
    {
        var result = MqttEnvelopeReader.TryRead(child, received.Message, out var value);

        if (result == EnvelopeReadResult.Malformed)
            _logger.LogEnvelopeValueNotReadable(child.Key, received.Message.Topic);

        var read = result == EnvelopeReadResult.Read;

        received.Values.Add(new()
        {
            Channel = child.Channel,
            Value = read ? value : InvalidValueOf(child.ValueType),
            Timestamp = received.Timestamp,
            Validity = read ? received.Validity : 0,
        });
    }

    private bool TryDeserializePayload(ReceivedMqttMessage received, INode node, Type? valueType, out object? value)
    {
        value = null;

        if (valueType is null)
        {
            _logger.LogPayloadNotReadable(received.Message.Topic, "unknown", null);
            return false;
        }

        try
        {
            var serializer = node.GetSerializer().ApplyDefault(_defaultSerializer);
            value = serializer.Deserialize(received.Message.Payload, valueType, _jsonOptions);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogPayloadNotReadable(received.Message.Topic, valueType.Name, ex);
            return false;
        }
    }

    private void ProcessGroupValue(ReceivedMqttMessage received, INode node, IReadOnlyCollection<INode[]> routes)
    {
        if (!TryReadPayloadAsJson(received, out var jsonMessage))
            return;

        foreach (var route in routes)
        {
            var dataPointNode = route[^1];
            var valueJson = NodeValueFactory.GetValueJsonNode(route, jsonMessage);

            if (valueJson is null)
                continue;

            // The data type its data point is configured with, never the one the publisher declares:
            // a member of a group message is read exactly as a message of its own would be.
            var valueType = dataPointNode.ValueType;

            if (TryDeserializeMember(received, valueJson, valueType, out var value))
                AddValue(received, node, dataPointNode.AffectedChannels, value);
        }
    }

    private bool TryReadPayloadAsJson(ReceivedMqttMessage received, out JsonNode? jsonMessage)
    {
        try
        {
            jsonMessage = received.Message.GetPayloadAsJsonNode();
            return true;
        }
        catch (JsonException ex)
        {
            _logger.LogPayloadNotReadable(received.Message.Topic, "JSON", ex);
            jsonMessage = null;
            return false;
        }
    }

    private bool TryDeserializeMember(ReceivedMqttMessage received, JsonNode valueJson, Type? valueType, out object? value)
    {
        value = null;

        if (valueType is null)
        {
            _logger.LogPayloadNotReadable(received.Message.Topic, "unknown", null);
            return false;
        }

        try
        {
            value = valueJson.Deserialize(valueType, _jsonOptions);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogPayloadNotReadable(received.Message.Topic, valueType.Name, ex);
            return false;
        }
    }

    private static void AddValue(ReceivedMqttMessage received, INode node, IReadOnlyCollection<string> affectedChannels, object? value)
        => received.Values.Add(new()
        {
            Channel = GetChannel(node, affectedChannels),
            Value = value,
            Timestamp = received.Timestamp,
            Validity = received.Validity,
        });

    /// <summary>
    /// The channel the value is forwarded on. The engine affects only channels a data point
    /// transfers, so which of the affected ones it is, is the only question left here.
    /// </summary>
    private static string GetChannel(INode node, IReadOnlyCollection<string> affectedChannels)
        => affectedChannels.First(node.TransferredChannels.Contains);
}
