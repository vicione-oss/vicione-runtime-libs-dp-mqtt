using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.Loader;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging;
using ViciOne.Suite.DataPort;

namespace Benchmarks;

[SuppressMessage("Maintainability", "CA1515:Erwägen Sie, öffentliche Typen intern zu machen.", Justification = "Must be public for BenchmarkDotNet")]
public class MqttDataPortCtorBenchmark : IDisposable
{
    private readonly List<Node> _nodes = [];
    private readonly List<Node> _nodesWithEnvelopeChildren = [];
    private bool _disposedValue;
    private readonly LoggerFactory _loggerFactory;

    public MqttDataPortCtorBenchmark() => _loggerFactory = new();

    [GlobalSetup]
    public void Setup()
    {
        for (var i = 0; i < NodeCount; i++)
        {
            var nodeId = Guid.NewGuid();
            Node columnNode = new()
            {
                Id = nodeId,
                DesignId = MqttNodeDesignId.Topic,
                Name = nodeId.ToString().Replace("-", string.Empty, StringComparison.InvariantCulture),
                AffectedChannels = { nodeId.ToString(), },
                TransferredChannels = { nodeId.ToString(), },
                ValueType = typeof(string),
            };
            _nodes.Add(columnNode);

            var childId = Guid.NewGuid();
            columnNode.TransferredChannels.Add(childId.ToString());
            _nodesWithEnvelopeChildren.Add(columnNode);
            _nodesWithEnvelopeChildren.Add(new()
            {
                Id = childId,
                ParentId = nodeId,
                DesignId = MqttNodeDesignId.UserProperty,
                Name = "batchId",
                AffectedChannels = { childId.ToString(), },
                ValueType = typeof(string),
            });
        }
    }

    [Params(1_000, 10_000)]
    public int NodeCount { get; set; }

    [Benchmark]
    [SuppressMessage("Naming", "CA1707:Bezeichner dürfen keine Unterstriche enthalten")]
    public void MqttDataPortOutgoing_ctor()
    {
        using MqttDataPortOutgoing _ = new(new()
        {
            Protocol = (byte)MqttProtocol.Tcp,
            Host = "localhost",
            Port = 1883,
            Nodes = _nodes,
        }, _loggerFactory, _loggerFactory.CreateLogger<MqttDataPortOutgoing>());
    }

    [Benchmark]
    [SuppressMessage("Naming", "CA1707:Bezeichner dürfen keine Unterstriche enthalten")]
    public void MqttDataPortOutgoing_ctor_with_envelope_children()
    {
        using MqttDataPortOutgoing _ = new(new()
        {
            Protocol = (byte)MqttProtocol.Tcp,
            Host = "localhost",
            Port = 1883,
            ProtocolVersion = 1,
            Nodes = _nodesWithEnvelopeChildren,
        }, _loggerFactory, _loggerFactory.CreateLogger<MqttDataPortOutgoing>());
    }

    [Benchmark]
    [SuppressMessage("Naming", "CA1707:Bezeichner dürfen keine Unterstriche enthalten")]
    public void MqttDataPortIncoming_ctor()
    {
        using MqttDataPortIncoming _ = new(new()
        {
            Protocol = (byte)MqttProtocol.Tcp,
            Host = "localhost",
            Port = 1883,
            Nodes = _nodes,
        }, _loggerFactory, _loggerFactory.CreateLogger<MqttDataPortIncoming>(), AssemblyLoadContext.Default);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
                _loggerFactory.Dispose();
            _disposedValue = true;
        }
    }
}
