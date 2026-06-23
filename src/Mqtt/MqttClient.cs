using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Extensions;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Internal;
using MQTTnet.Protocol;

namespace ViciOne.Suite.DataPort;

internal sealed class MqttClient(IManagedMqttClient managedMqttClient, CommunicationInfo communicationInfo) : IVirtualMqttClient
{
    private readonly AsyncEvent<MqttApplicationMessageReceivedEventArgs> _messageReceived = new();

    public IManagedMqttClient InnerClient => managedMqttClient;

    [SuppressMessage("Reliability", "CA2000:Objekte verwerfen, bevor Bereich verloren geht")]
    internal static MqttClient Create(CommunicationInfo communicationInfo, ILoggerFactory loggerFactory)
        => new(new MqttClientFactory().CreateManagedMqttClient(new MqttLogger(loggerFactory.CreateLogger<IManagedMqttClient>())), communicationInfo);

    public event Func<MqttApplicationMessageReceivedEventArgs, Task>? MessageReceived
    {
        add => _messageReceived.AddHandler(value);
        remove => _messageReceived.RemoveHandler(value);
    }

    public Task Connect()
    {
        managedMqttClient.ApplicationMessageReceivedAsync += ReceiveMessage;
        return managedMqttClient.StartAsync(communicationInfo.BuildClientOptions());
    }

    public Task Disconnect()
        => managedMqttClient.StopAsync();

    public Task Publish(MqttApplicationMessage message)
        => managedMqttClient.EnqueueAsync(message);

    public Task ReceiveMessage(MqttApplicationMessageReceivedEventArgs eventArgs)
        => _messageReceived.InvokeAsync(eventArgs);

    public Task Subscribe(string topicFilter, MqttQualityOfServiceLevel qos, bool retain)
        => managedMqttClient.SubscribeAsync(topicFilter, qos);

    public Task Unsubscribe(string topicFilter)
        => managedMqttClient.UnsubscribeAsync(topicFilter);

    public void Dispose()
        => managedMqttClient.Dispose();
}
