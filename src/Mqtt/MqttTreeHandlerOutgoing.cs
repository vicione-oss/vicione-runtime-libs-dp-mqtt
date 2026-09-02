using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Extensions;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using ViciOne.ManagedEngine.ExternalCommunication;

namespace ViciOne.Suite.DataPort;

internal sealed class MqttTreeHandlerOutgoing : UnspecificTreeHandlerOutgoing<MqttApplicationMessageBuilder>
{
    private readonly Dictionary<Guid, string> _nodeTopics = [];
    private readonly Serializer _defaultSerializer;
    private readonly bool _supportsExtendedProtocol;
    private readonly MqttQualityOfServiceLevel _qualityOfService;
    private readonly MqttDataPortCommunication _communication;
    private readonly IVirtualMqttClient _client;

    public MqttTreeHandlerOutgoing(MqttDataPortCommunication communication, IVirtualMqttClient client)
    {
        _communication = communication;
        _client = client;
        MqttDataPortProperties properties = new(_communication);
        _supportsExtendedProtocol = properties.ProtocolVersion == MqttProtocolVersion.V500;
        _qualityOfService = properties.QualityOfService;
        _defaultSerializer = properties.DefaultSerializer;
    }

    public override void Initialize()
    {
        _ = EnvelopeChildren.Create(_communication.Nodes, _supportsExtendedProtocol);

        var idNodes = _communication.Nodes.ToDictionary(n => n.Id, n => (INode)n);
        Initialize(_communication.Nodes, node => _nodeTopics.Add(node.Id, GenerateTopic(node.GetRoute(idNodes))));
    }

    protected override MqttApplicationMessageBuilder HandleGroupNode(INode node, JsonObject data, JsonObject meta, IReadOnlyCollection<ExternalValue> values)
    {
        var serializer = node.GetSerializer().ApplyDefault(_defaultSerializer);
        var payload = JsonSerializer.SerializeToUtf8Bytes(data);
        return CreateDefaultBuilder(node, serializer, payload);
    }

    protected override MqttApplicationMessageBuilder HandleSingleNode(INode node, ExternalValue value)
    {
        var serializer = node.GetSerializer().ApplyDefault(_defaultSerializer);
        var type = value.Value?.GetType() ?? typeof(object);
        var payload = serializer.Serialize(value.Value, type, JsonSetup.PreserveTypeOptions);
        return CreateDefaultBuilder(node, serializer, payload);
    }

    private MqttApplicationMessageBuilder CreateDefaultBuilder(INode node, Serializer serializer, byte[] payload)
    {
        var builder = new MqttApplicationMessageBuilder()
            .WithTopic(_nodeTopics[node.Id])
            .WithQualityOfServiceLevel(_qualityOfService)
            .WithPayload(payload);
        if (node.TryGetProperty(MqttNodeProperties.Retain, out var property) && property.GetValue<bool>())
            builder.WithRetainFlag();
        if (_supportsExtendedProtocol)
        {
            builder
                .WithPayloadFormatIndicator(MqttPayloadFormatIndicator.CharacterData)
                .WithContentType(serializer.ToContentType());
        }
        return builder;
    }

    protected override async Task SendValue(MqttApplicationMessageBuilder value, CancellationToken cancellationToken)
        => await _client.Publish(value.Build());

    private static string GenerateTopic(IEnumerable<INode> nodes)
        => nodes
            .Where(n => !string.IsNullOrWhiteSpace(n.Name))
            .Select(n => n.Name)
            .Aggregate((a, b) => a + "/" + b);
}
