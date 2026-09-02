using System;
using System.Collections.Generic;
using System.Linq;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// The envelope children of a node tree, resolved once at initialization.
/// </summary>
/// <remarks>
/// Building this rejects a tree the port cannot serve, so a misconfigured port fails to start
/// instead of publishing something surprising for the rest of its life.
/// </remarks>
internal sealed class EnvelopeChildren
{
    /// <summary>
    /// How many user properties this port puts on one message. The ruleset offers the same number
    /// of <c>User property</c> children per data point.
    /// </summary>
    internal const int MaxUserProperties = 16;

    private static readonly EnvelopeChild[] s_none = [];

    internal static EnvelopeChildren Empty { get; } = new([], []);

    private readonly Dictionary<Guid, EnvelopeChild[]> _byParent;
    private readonly Dictionary<string, EnvelopeChild> _byChannel;

    private EnvelopeChildren(Dictionary<Guid, EnvelopeChild[]> byParent, Dictionary<string, EnvelopeChild> byChannel)
    {
        _byParent = byParent;
        _byChannel = byChannel;
    }

    internal bool IsEmpty => _byParent.Count == 0;

    internal IReadOnlyList<EnvelopeChild> Of(Guid parentId)
        => _byParent.TryGetValue(parentId, out var children) ? children : s_none;

    internal bool TryGetChild(string channel, out EnvelopeChild child)
        => _byChannel.TryGetValue(channel, out child!);

    internal static EnvelopeChildren Create(IReadOnlyCollection<INode> nodes, bool supportsUserProperties)
    {
        var nodesById = nodes.ToDictionary(n => n.Id);
        var childrenByParent = GroupByParent(nodes, nodesById);
        var violations = FindViolations(childrenByParent, nodesById, supportsUserProperties);

        if (violations.Count != 0)
            throw new InvalidOperationException($"The MQTT data port cannot be started with this configuration:{Environment.NewLine}- {string.Join($"{Environment.NewLine}- ", violations)}");

        return new(childrenByParent, MapChannels(childrenByParent));
    }

    private static Dictionary<Guid, EnvelopeChild[]> GroupByParent(IReadOnlyCollection<INode> nodes, Dictionary<Guid, INode> nodesById)
    {
        Dictionary<Guid, List<EnvelopeChild>> childrenByParent = [];

        foreach (var node in nodes)
        {
            var kind = GetKind(node);

            if (kind is null || node.ParentId is not { } parentId)
                continue;

            if (!childrenByParent.TryGetValue(parentId, out var children))
                childrenByParent.Add(parentId, children = []);

            nodesById.TryGetValue(parentId, out var parent);
            children.Add(EnvelopeChild.Create(node, kind.Value, GetChannel(node, parent)));
        }

        return childrenByParent.ToDictionary(e => e.Key, e => e.Value.ToArray());
    }

    /// <summary>
    /// Indexes the children the engine exchanges a value with. A fixed child the engine never links
    /// has no channel of its own, and a value can never arrive on one, so it is left out rather
    /// than filed under the empty channel every one of them would share.
    /// </summary>
    private static Dictionary<string, EnvelopeChild> MapChannels(Dictionary<Guid, EnvelopeChild[]> childrenByParent)
    {
        Dictionary<string, EnvelopeChild> byChannel = [];

        foreach (var (_, children) in childrenByParent)
        {
            foreach (var child in children)
            {
                if (child.Channel.Length != 0)
                    byChannel.Add(child.Channel, child);
            }
        }

        return byChannel;
    }

    private static string GetChannel(INode node, INode? parent)
    {
        if (parent is null)
            return string.Empty;

        foreach (var channel in node.AffectedChannels)
        {
            if (parent.TransferredChannels.Contains(channel))
                return channel;
        }

        return string.Empty;
    }

    private static List<string> FindViolations(Dictionary<Guid, EnvelopeChild[]> childrenByParent, Dictionary<Guid, INode> nodesById, bool supportsUserProperties)
    {
        List<string> violations = [];

        foreach (var (parentId, children) in childrenByParent)
        {
            if (!nodesById.TryGetValue(parentId, out var parent))
            {
                AddOrphanViolations(children, violations);
                continue;
            }

            if (GetKind(parent) is not null)
            {
                violations.Add($"'{parent.Name}' is an envelope child and cannot have envelope children of its own.");
                continue;
            }

            if (!supportsUserProperties)
                violations.Add($"'{parent.Name}' has envelope children, which MQTT 3.1.1 cannot carry. Use MQTT 5.0 or remove them.");

            if (!parent.AffectedChannels.Any(parent.TransferredChannels.Contains))
                violations.Add($"'{parent.Name}' has envelope children but transfers no value of its own, so there is no message to put them on.");

            AddKeyViolations(parent, children, violations);
        }

        AddChannelViolations(childrenByParent, violations);

        return violations;
    }

    private static void AddOrphanViolations(EnvelopeChild[] children, List<string> violations)
    {
        foreach (var child in children)
            violations.Add($"'{child.Node.Name}' is an envelope child of a data point that is not part of this data port.");
    }

    private static void AddKeyViolations(INode parent, EnvelopeChild[] children, List<string> violations)
    {
        HashSet<string> keys = new(StringComparer.OrdinalIgnoreCase);
        var userProperties = 0;

        foreach (var child in children)
        {
            if (!keys.Add(child.Key))
                violations.Add($"'{parent.Name}' has more than one envelope child for the key '{child.Key}'.");

            // Only a user property carries a value the engine must supply; the fixed children are
            // derived from the parent, and a Timestamp is written by the port outbound and only
            // read by the engine inbound.
            if (child.Channel.Length == 0 && child.Kind == EnvelopeChildKind.UserProperty)
                violations.Add($"'{child.Node.Name}' is an envelope child of '{parent.Name}', which transfers none of its channels, so its value can never be exchanged.");

            if (child.Kind == EnvelopeChildKind.UserProperty)
                userProperties++;

            if (child.Kind == EnvelopeChildKind.UserProperty && IsReserved(child.Key))
                violations.Add($"'{child.Node.Name}' under '{parent.Name}' uses the reserved envelope key '{child.Key}'.");
        }

        if (userProperties > MaxUserProperties)
            violations.Add($"'{parent.Name}' has {userProperties} user properties, which is more than the {MaxUserProperties} this data port puts on a message.");
    }

    /// <summary>
    /// Two children on the same channel would read and write the same engine value under two keys,
    /// of which only the last one could be served.
    /// </summary>
    private static void AddChannelViolations(Dictionary<Guid, EnvelopeChild[]> childrenByParent, List<string> violations)
    {
        Dictionary<string, EnvelopeChild> byChannel = [];

        foreach (var (_, children) in childrenByParent)
        {
            foreach (var child in children)
            {
                if (child.Channel.Length != 0 && !byChannel.TryAdd(child.Channel, child))
                    violations.Add($"'{byChannel[child.Channel].Node.Name}' and '{child.Node.Name}' are envelope children of the same value, which a message cannot carry twice.");
            }
        }
    }

    private static bool IsReserved(string key)
        => key.Equals(MqttUserProperties.Timestamp, StringComparison.OrdinalIgnoreCase)
        || key.Equals(MqttUserProperties.Validity, StringComparison.OrdinalIgnoreCase)
        || key.Equals(MqttUserProperties.Type, StringComparison.OrdinalIgnoreCase);

    private static EnvelopeChildKind? GetKind(INode node)
        => node.DesignId switch
        {
            MqttNodeDesignId.UserProperty => EnvelopeChildKind.UserProperty,
            MqttNodeDesignId.Timestamp => EnvelopeChildKind.Timestamp,
            MqttNodeDesignId.Validity => EnvelopeChildKind.Validity,
            MqttNodeDesignId.Type => EnvelopeChildKind.Type,
            _ => null,
        };
}
