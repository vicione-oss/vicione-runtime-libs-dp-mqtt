using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Loader;
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
    private readonly NamedTypeResolver _namedTypes;
    private readonly DistinctReports _reports = new();
    private readonly Serializer _defaultSerializer;
    private readonly EnvelopeChildren _envelopeChildren;
    private readonly ILogger<MqttDataPortIncoming> _logger;

    internal MqttMessageHandler(
        Dictionary<Guid, IReadOnlyCollection<INode[]>> jsonRoutesByNode,
        Dictionary<string, IReadOnlyCollection<INode>> nodesByTopic,
        TimeProvider timeProvider,
        JsonSerializerOptions jsonOptions,
        AssemblyLoadContext loadContext,
        Serializer defaultSerializer,
        EnvelopeChildren envelopeChildren,
        ILogger<MqttDataPortIncoming> logger)
    {
        _jsonRoutesByNode = jsonRoutesByNode;
        _nodesByTopic = nodesByTopic;
        _timeProvider = timeProvider;
        _jsonOptions = jsonOptions;
        _namedTypes = new(loadContext);
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

        if (received.UnreadableTimestamp is { } text && _reports.IsFirst(MqttUserProperties.Timestamp, topic, text))
            _logger.LogTimestampNotReadable(text, topic);

        if (received.UnreadableValidity is { } validity && _reports.IsFirst(MqttUserProperties.Validity, topic, validity))
            _logger.LogValidityNotReadable(validity, topic);

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
        if (TryDeserializePayload(received, node, ResolveValueType(node.ValueType, received.Message.ReadValueTypeName(), received.Message.Topic), out var value))
            AddValue(received, node, node.AffectedChannels, value);

        AddEnvelopeChildren(received, node);
    }

    /// <summary>
    /// Forwards one value per envelope child of <paramref name="node"/> the message carries a
    /// readable value for, in the order the tree declares them, so a received message reaches the
    /// engine as the data point and its children together. <see cref="EnvelopeChildKind.Validity"/>
    /// and <see cref="EnvelopeChildKind.Type"/> are outbound only, so neither is a fan-out target;
    /// the validity of the received message reaches the engine as the validity of the data point
    /// and of every child read from it, so a child is never more valid than the message that
    /// carried it. A message that declares no validity is valid, as it has always been.
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

    /// <summary>
    /// Forwards the value of <paramref name="child"/> when the message carries one. A key the
    /// message leaves out, or one whose text is not a value of the child's data type, forwards
    /// nothing: the default of the data type is a reading the sender could have taken, so the
    /// engine could not tell the two apart from the value alone.
    /// </summary>
    private void AddEnvelopeChild(ReceivedMqttMessage received, EnvelopeChild child)
    {
        var result = MqttEnvelopeReader.TryRead(child, received.Message, out var value);

        // A Timestamp child reads the very property the message itself was read from, which is
        // reported once when the message arrives. Reporting it here too would say the same thing
        // twice about one text.
        if (result == EnvelopeReadResult.Malformed && child.Kind != EnvelopeChildKind.Timestamp)
            ReportEnvelopeValueNotReadable(received.Message.Topic, child.Key);

        if (result != EnvelopeReadResult.Read)
            return;

        received.Values.Add(new()
        {
            Channel = child.Channel,
            Value = value,
            Timestamp = received.Timestamp,
            Validity = received.Validity,
        });
    }

    /// <summary>
    /// The data type a value is read as: the one its data point declares, or the more derived
    /// one the sender named in the <c>Type</c> user property. The declared type stays the contract,
    /// so a sender may narrow what it sends — which is what carries a derived type across a link of
    /// two of these ports — but never decide which type this port loads: a named type counts only if
    /// its assembly is loaded already.
    /// </summary>
    /// <remarks>
    /// A name that resolves to something the declared type cannot hold is not worth reporting: the
    /// outgoing side names the runtime type of the value it published, which is
    /// <see cref="object"/> for a null and may be a narrower primitive than the tree declares — an
    /// <c>int</c> for an <c>Int64</c> data point — so a name that is not a subtype is ordinary
    /// traffic between two of these ports. Neither is an abstract one: the name always comes from a
    /// value that existed, so an abstract type or an interface was written by no port and could not
    /// be deserialized into anyway. A name that resolves to nothing at all is the one that says
    /// something is wrong — the sender knows a type this port does not.
    /// </remarks>
    private Type? ResolveValueType(Type? declared, string? name, string topic)
    {
        if (declared is null || name is null)
            return declared;

        if (name == declared.AssemblyQualifiedName)
            return declared;

        var named = _namedTypes.Resolve(name);

        if (named is null && _reports.IsFirst(MqttUserProperties.Type, topic, name))
            _logger.LogNamedValueTypeNotLoadable(name, topic);

        return named is { IsAbstract: false } && declared.IsAssignableFrom(named) ? named : declared;
    }

    private bool TryDeserializePayload(ReceivedMqttMessage received, INode node, Type? valueType, out object? value)
    {
        value = null;

        if (valueType is null)
        {
            ReportPayloadNotReadable(received.Message.Topic, "unknown", null);
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
            ReportPayloadNotReadable(received.Message.Topic, valueType.Name, ex);
            return false;
        }
    }

    private void ProcessGroupValue(ReceivedMqttMessage received, INode node, IReadOnlyCollection<INode[]> routes)
    {
        if (!TryReadPayloadAsJson(received, out var jsonMessage))
            return;

        var memberTypeNames = received.Message.ReadMemberTypeNames();

        foreach (var route in routes)
        {
            var dataPointNode = route[^1];
            var valueJson = NodeValueFactory.GetValueJsonNode(route, jsonMessage);

            if (valueJson is null)
                continue;

            var valueType = ResolveValueType(dataPointNode.ValueType, ReadMemberTypeName(route, memberTypeNames), received.Message.Topic);

            if (TryDeserializeMember(received, valueJson, valueType, out var value))
                AddValue(received, node, dataPointNode.AffectedChannels, value);
        }
    }

    private static string? ReadMemberTypeName(INode[] route, JsonObject? memberTypeNames)
        => NodeValueFactory.GetValueJsonNode(route, memberTypeNames) is JsonValue entry && entry.TryGetValue(out string? name) ? name : null;

    private bool TryReadPayloadAsJson(ReceivedMqttMessage received, out JsonNode? jsonMessage)
    {
        try
        {
            jsonMessage = received.Message.GetPayloadAsJsonNode();
            return true;
        }
        catch (JsonException ex)
        {
            ReportPayloadNotReadable(received.Message.Topic, "JSON", ex);
            jsonMessage = null;
            return false;
        }
    }

    private bool TryDeserializeMember(ReceivedMqttMessage received, JsonNode valueJson, Type? valueType, out object? value)
    {
        value = null;

        if (valueType is null)
        {
            ReportPayloadNotReadable(received.Message.Topic, "unknown", null);
            return false;
        }

        try
        {
            value = valueJson.Deserialize(valueType, _jsonOptions);
            return true;
        }
        catch (Exception ex)
        {
            ReportPayloadNotReadable(received.Message.Topic, valueType.Name, ex);
            return false;
        }
    }

    private void ReportEnvelopeValueNotReadable(string topic, string key)
    {
        if (_reports.IsFirst(nameof(ReportEnvelopeValueNotReadable), topic, key))
            _logger.LogEnvelopeValueNotReadable(key, topic);
    }

    private void ReportPayloadNotReadable(string topic, string valueType, Exception? exception)
    {
        if (_reports.IsFirst(nameof(ReportPayloadNotReadable), topic, valueType))
            _logger.LogPayloadNotReadable(topic, valueType, exception);
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
