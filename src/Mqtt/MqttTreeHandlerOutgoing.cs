using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    private readonly Dictionary<Guid, ExternalValue> _lastChildValues = [];
    private EnvelopeChildren _envelopeChildren = EnvelopeChildren.Empty;
    private Task _lastSend = Task.CompletedTask;

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
        _envelopeChildren = EnvelopeChildren.Create(_communication.Nodes, _supportsExtendedProtocol);

        var idNodes = _communication.Nodes.ToDictionary(n => n.Id, n => (INode)n);
        Initialize(_communication.Nodes, node => _nodeTopics.Add(node.Id, GenerateTopic(node.GetRoute(idNodes))));
    }

    /// <summary>
    /// Publishes one message per data point in <paramref name="values"/>. Envelope children never
    /// produce a message of their own; their value is remembered and put on the next message of
    /// their parent.
    /// </summary>
    /// <remarks>
    /// Hides the inherited method, which files a child under the parent that transfers its channel
    /// and so fails on a batch that carries both. The children are taken out here and the rest is
    /// published by the inherited method.
    /// The engine sends consecutive cycles without waiting for the previous one, so batches can
    /// overlap. They are chained onto each other here, which keeps the remembered child values in
    /// step with the messages they belong to and publishes the cycles in the order they arrived.
    /// </remarks>
    public new Task Send(IEnumerable<ExternalValue> values, CancellationToken cancellationToken)
    {
        TaskCompletionSource finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var previous = Interlocked.Exchange(ref _lastSend, finished.Task);

        return SendAfter(previous, finished, values, cancellationToken);
    }

    private async Task SendAfter(Task previous, TaskCompletionSource finished, IEnumerable<ExternalValue> values, CancellationToken cancellationToken)
    {
        try
        {
            await previous.ConfigureAwait(false);
            await base.Send(RememberChildValues(values), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            finished.SetResult();
        }
    }

    /// <summary>
    /// Remembers the value of every envelope child in <paramref name="values"/> and returns the
    /// values of the data points, which are the ones that make a message.
    /// </summary>
    private List<ExternalValue> RememberChildValues(IEnumerable<ExternalValue> values)
    {
        List<ExternalValue> dataPointValues = [];

        foreach (var value in values)
        {
            if (_envelopeChildren.TryGetChild(value.Channel, out var child))
                _lastChildValues[child.Node.Id] = value;
            else
                dataPointValues.Add(value);
        }

        return dataPointValues;
    }

    private void AddEnvelope(MqttApplicationMessageBuilder builder, INode node, ExternalValue value)
    {
        var children = _envelopeChildren.Of(node.Id);

        for (var i = 0; i < children.Count; i++)
        {
            var child = children[i];

            switch (child.Kind)
            {
                case EnvelopeChildKind.UserProperty:
                    AddUserProperty(builder, child);
                    break;
                case EnvelopeChildKind.Timestamp:
                    AddProperty(builder, child.Key, MqttEnvelopeCodec.FormatTimestamp(value.Timestamp));
                    break;
                case EnvelopeChildKind.Validity:
                    AddProperty(builder, child.Key, MqttEnvelopeCodec.FormatValidity(value.Validity));
                    break;
                case EnvelopeChildKind.Type:
                    AddProperty(builder, child.Key, (value.Value?.GetType() ?? typeof(object)).AssemblyQualifiedName!);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(node), child.Kind, "Unknown envelope child kind.");
            }
        }
    }

    private static void AddProperty(MqttApplicationMessageBuilder builder, string key, string value)
        => builder.WithUserProperty(key, Encoding.UTF8.GetBytes(value));

    private void AddUserProperty(MqttApplicationMessageBuilder builder, EnvelopeChild child)
    {
        // An invalid value is held back rather than dropped: it stays cached, so the key reappears
        // as soon as the engine writes a valid one.
        if (_lastChildValues.TryGetValue(child.Node.Id, out var value) && value.Validity != 0)
            AddProperty(builder, child.Key, MqttEnvelopeCodec.Format(value.Value));
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
        var builder = CreateDefaultBuilder(node, serializer, payload);
        AddEnvelope(builder, node, value);
        return builder;
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
