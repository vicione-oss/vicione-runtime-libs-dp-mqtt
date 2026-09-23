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

/// <summary>
/// Publishes the values of a send cycle. The tree handler of the dps library does the publishing,
/// but it files an envelope child under the parent that transfers its channel and so fails on a
/// batch that carries both. It is therefore held privately in <see cref="Publisher"/> and handed the
/// values of the data points only, so no caller can reach it with a child.
/// </summary>
internal sealed class MqttTreeHandlerOutgoing
{
    private readonly MqttDataPortCommunication _communication;
    private readonly bool _supportsExtendedProtocol;
    private readonly Publisher _publisher;
    private readonly Dictionary<Guid, ExternalValue> _lastChildValues = [];
    private EnvelopeChildren _envelopeChildren = EnvelopeChildren.Empty;
    private Task _lastSend = Task.CompletedTask;

    public MqttTreeHandlerOutgoing(MqttDataPortCommunication communication, IVirtualMqttClient client)
    {
        _communication = communication;
        MqttDataPortProperties properties = new(_communication);
        _supportsExtendedProtocol = properties.ProtocolVersion == MqttProtocolVersion.V500;
        _publisher = new(this, communication, client, properties, _supportsExtendedProtocol);
    }

    public void Initialize()
    {
        _envelopeChildren = EnvelopeChildren.Create(_communication.Nodes, _supportsExtendedProtocol);
        _publisher.Initialize();
    }

    /// <summary>
    /// Publishes one message per data point in <paramref name="values"/>. Envelope children never
    /// produce a message of their own; their value is remembered and put on the next message of
    /// their parent.
    /// </summary>
    /// <remarks>
    /// The engine sends consecutive cycles without waiting for the previous one, so batches can
    /// overlap. They are chained onto each other here, which keeps the remembered child values in
    /// step with the messages they belong to and publishes the cycles in the order they arrived.
    /// </remarks>
    public Task Send(IEnumerable<ExternalValue> values, CancellationToken cancellationToken)
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
            await _publisher.Send(RememberChildValues(values), cancellationToken).ConfigureAwait(false);
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
        // A user property carries no validity of its own on the wire, so the one the engine
        // wrote is not read back as a rule about whether to publish: the tree decides the keys.
        if (_lastChildValues.TryGetValue(child.Node.Id, out var value))
            AddProperty(builder, child.Key, MqttEnvelopeCodec.Format(value.Value));
    }

    private sealed class Publisher(
        MqttTreeHandlerOutgoing owner,
        MqttDataPortCommunication communication,
        IVirtualMqttClient client,
        MqttDataPortProperties properties,
        bool supportsExtendedProtocol)
        : UnspecificTreeHandlerOutgoing<MqttApplicationMessageBuilder>
    {
        private readonly Dictionary<Guid, string> _nodeTopics = [];
        private readonly Serializer _defaultSerializer = properties.DefaultSerializer;
        private readonly MqttQualityOfServiceLevel _qualityOfService = properties.QualityOfService;

        public override void Initialize()
        {
            var idNodes = communication.Nodes.ToDictionary(n => n.Id, n => (INode)n);
            Initialize(communication.Nodes, node => _nodeTopics.Add(node.Id, GenerateTopic(node.GetRoute(idNodes))));
        }

        protected override MqttApplicationMessageBuilder HandleGroupNode(INode node, JsonObject data, JsonObject meta, IReadOnlyCollection<ExternalValue> values)
        {
            var serializer = node.GetSerializer().ApplyDefault(_defaultSerializer);
            var payload = JsonSerializer.SerializeToUtf8Bytes(data);
            var builder = CreateDefaultBuilder(node, serializer, payload);
            // A folder cannot declare envelope children, so its message keeps the envelope 1.0.0 sent.
            if (supportsExtendedProtocol)
            {
                AddProperty(builder, MqttUserProperties.Timestamp, MqttEnvelopeCodec.FormatTimestamp(values.Max(v => v.Timestamp)));
                AddProperty(builder, MqttUserProperties.Validity, MqttEnvelopeCodec.FormatValidity(values.Min(v => v.Validity)));
                AddProperty(builder, MqttUserProperties.Type, JsonSerializer.Serialize(meta));
            }
            return builder;
        }

        protected override MqttApplicationMessageBuilder HandleSingleNode(INode node, ExternalValue value)
        {
            var serializer = node.GetSerializer().ApplyDefault(_defaultSerializer);
            var type = value.Value?.GetType() ?? typeof(object);
            var payload = serializer.Serialize(value.Value, type, JsonSetup.PreserveTypeOptions);
            var builder = CreateDefaultBuilder(node, serializer, payload);
            owner.AddEnvelope(builder, node, value);
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
            if (supportsExtendedProtocol)
            {
                builder
                    .WithPayloadFormatIndicator(MqttPayloadFormatIndicator.CharacterData)
                    .WithContentType(serializer.ToContentType());
            }
            return builder;
        }

        /// <summary>
        /// The client takes no cancellation token of its own, so the cycle's token is observed here:
        /// no further message is handed to it once the cycle is cancelled, and one it has already
        /// taken but never answers is given up on rather than holding the cycle, and every cycle
        /// chained behind it, for the life of the port. The message given up on may still reach the
        /// broker; what ends is this port's wait for it.
        /// </summary>
        protected override async Task SendValue(MqttApplicationMessageBuilder value, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await client.Publish(value.Build()).WaitAsync(cancellationToken);
        }

        private static string GenerateTopic(IEnumerable<INode> nodes)
            => nodes
                .Where(n => !string.IsNullOrWhiteSpace(n.Name))
                .Select(n => n.Name)
                .Aggregate((a, b) => a + "/" + b);
    }
}
