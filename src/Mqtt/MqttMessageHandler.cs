using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using MQTTnet.Extensions;
using ViciOne.ManagedEngine.ExternalCommunication;

namespace ViciOne.Suite.DataPort;

internal static class MqttMessageHandler
{
    internal static List<ExternalValue> HandleMessage(MQTTnet.MqttApplicationMessageReceivedEventArgs eventArgs,
        Dictionary<Guid, IReadOnlyCollection<INode[]>> transferredNodeJsonNodes,
        Dictionary<string, IReadOnlyCollection<INode>> addressNodes, TimeProvider timeProvider, JsonSerializerOptions jsonOptions, Serializer defaultSerializer)
    {
        var timestamp = eventArgs.ApplicationMessage.GetTimestamp(timeProvider).DateTime;
        var validity = eventArgs.ApplicationMessage.GetValidity();
        var typeProperty = eventArgs.ApplicationMessage.UserProperties?.FindOptional(MqttUserProperties.Type);
        var metaJson = typeProperty?.GetAsJsonObject();

        var transferredNodes = addressNodes[eventArgs.ApplicationMessage.Topic];
        List<ExternalValue> values = [];

        foreach (var transferredNode in transferredNodes)
        {
            var serializer = transferredNode.GetSerializer().ApplyDefault(defaultSerializer);

            if (transferredNodeJsonNodes.TryGetValue(transferredNode.Id, out var routes))
            {
                var jsonMessage = eventArgs.ApplicationMessage.GetPayloadAsJsonNode();
                ProcessGroupValue(transferredNode, jsonMessage, routes);
            }
            else
            {
                ProcessValue(transferredNode, serializer, eventArgs.ApplicationMessage.Payload, typeProperty?.GetAsType() ?? transferredNode.ValueType, transferredNode.AffectedChannels);
            }
        }

        return values;

        void ProcessValue(INode transferredNode, Serializer serializer, ReadOnlySequence<byte> data, Type? nodeType, IReadOnlyCollection<string> affectedChannels)
        {
            if (nodeType is null)
                throw new InvalidOperationException("Type of value not found.");

            var value = serializer.Deserialize(data, nodeType, jsonOptions);
            var channel = affectedChannels.First(transferredNode.TransferredChannels.Contains);

            values.Add(new()
            {
                Channel = channel,
                Value = value,
                Timestamp = timestamp,
                Validity = validity,
            });
        }

        void ProcessJsonValue(INode transferredNode, JsonNode? node, Type? nodeType, IReadOnlyCollection<string> affectedChannels)
        {
            if (node is null)
                return;
            if (nodeType is null)
                throw new InvalidOperationException("Type of value not found.");

            var value = node.Deserialize(nodeType, jsonOptions);
            var channel = affectedChannels.First(transferredNode.TransferredChannels.Contains);

            values.Add(new()
            {
                Channel = channel,
                Value = value,
                Timestamp = timestamp,
                Validity = validity,
            });
        }

        void ProcessGroupValue(INode transferredNode, JsonNode? jsonMessage, IReadOnlyCollection<INode[]> routes)
        {
            foreach (var route in routes)
            {
                var dataPortNode = route[^1];
                var valueNode = NodeValueFactory.GetValueJsonNode(route, jsonMessage);
                var valueType = NodeValueFactory.GetValueJsonNode(route, metaJson)?.GetValue<string>().ToType();
                valueType ??= dataPortNode.ValueType;

                ProcessJsonValue(transferredNode, valueNode, valueType, dataPortNode.AffectedChannels);
            }
        }
    }
}
