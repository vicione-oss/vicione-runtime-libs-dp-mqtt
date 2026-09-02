using System.Linq;
using System.Reflection;
using AwesomeAssertions;
using ViciOne.Tree.Builder.Extensions;
using ViciOne.Tree.Builder.NodeTypes;
using ViciOne.Tree.Builder.Rules.Yaml;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class YamlTests
{
    [Fact]
    public void All_properties_of_communication_class_have_a_matching_yaml_property()
    {
        var metadata = RulesDeserializer.Deserialize("Mqtt.yaml");
        var dataPortCommunicationType = typeof(MqttDataPortCommunication);

        var yamlPropertyIds = metadata.PropertyTypes
            .Select(p => p.Id)
            .ToHashSet();

        var yamlNodeTypeIds = metadata.NodeTypes
            .Select(n => n.Id)
            .ToHashSet();

        var classPropertyNames = dataPortCommunicationType
            .GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Select(p => p.Name)
            .ToHashSet();

        classPropertyNames.Except(yamlPropertyIds.Union(yamlNodeTypeIds))
            .Should().BeEmpty();
    }

    [Fact]
    public void DesignIds_should_be_in_yaml()
    {
        var metadata = RulesDeserializer.Deserialize("Mqtt.yaml");
        var nodeDesignIdType = typeof(MqttNodeDesignId);

        var yamlDesignIds = metadata.NodeTypes
            .OfType<DataPortTreeNodeType>()
            .Where(n => !string.IsNullOrEmpty(n.MappingId))
            .Select(n => n.MappingId)
            .ToHashSet();

        var expectedDesignIds = nodeDesignIdType
            .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(f => f.IsLiteral && !f.IsInitOnly && f.FieldType == typeof(string))
            .Select(f => (string)f.GetValue(null)!)
            .ToHashSet();

        expectedDesignIds.Except(yamlDesignIds)
            .Should().BeEmpty();
    }

    [Theory]
    [InlineData("DataPointBool")]
    [InlineData("DataPointInteger")]
    [InlineData("DataPointFloat")]
    [InlineData("DataPointDateTime")]
    [InlineData("DataPointString")]
    [InlineData("DataPointBinary")]
    public void Offers_the_envelope_children_below_every_data_point(string nodeTypeId)
    {
        var metadata = RulesDeserializer.Deserialize("Mqtt.yaml");

        metadata.IsEnvelopeParent(nodeTypeId).Should().BeTrue();
        metadata.GetEnvelopeChildren(nodeTypeId).Select(c => c.Id).Should().Equal(
            MqttNodeDesignId.UserProperty,
            MqttNodeDesignId.Timestamp,
            MqttNodeDesignId.Validity,
            MqttNodeDesignId.Type);

        var childNodes = metadata.NodeTypes
            .Single(n => n.Id == nodeTypeId)
            .ChildNodes;

        childNodes.Single(c => c.Id == MqttNodeDesignId.UserProperty).MaxInstances.Should().Be(16);
        childNodes.Where(c => c.Id != MqttNodeDesignId.UserProperty).Should().OnlyContain(c => c.MaxInstances == 1);
    }

    /// <summary>
    /// Asked the way the cluster editor and the deployment ask it: which way the value of a fixed
    /// child travels, and in which of those directions the engine may link it.
    /// </summary>
    [Theory]
    [InlineData(MqttNodeDesignId.Timestamp, new[] { DataPortTransferDirection.Outbound, DataPortTransferDirection.Inbound, }, new[] { DataPortTransferDirection.Inbound, })]
    [InlineData(MqttNodeDesignId.UserProperty, new[] { DataPortTransferDirection.Outbound, DataPortTransferDirection.Inbound, }, new[] { DataPortTransferDirection.Outbound, DataPortTransferDirection.Inbound, })]
    [InlineData(MqttNodeDesignId.Validity, new[] { DataPortTransferDirection.Outbound, }, new DataPortTransferDirection[0])]
    [InlineData(MqttNodeDesignId.Type, new[] { DataPortTransferDirection.Outbound, }, new DataPortTransferDirection[0])]
    public void Declares_the_directions_of_an_envelope_child(string childId, DataPortTransferDirection[] transfer, DataPortTransferDirection[] link)
    {
        var metadata = RulesDeserializer.Deserialize("Mqtt.yaml");

        var parents = metadata.GetEnvelopeParents(childId);

        parents.Should().NotBeEmpty();
        parents.Should().AllSatisfy(parent =>
        {
            metadata.GetEnvelopeTransferDirections(parent.Id, childId).Should().BeEquivalentTo(transfer);
            metadata.GetEnvelopeLinkDirections(parent.Id, childId).Should().BeEquivalentTo(link);
        });
    }

    /// <summary>
    /// The children the port derives from the parent value are sent but never linked, so the editor
    /// offers no connector for them and a cluster that links one is rejected at deployment.
    /// </summary>
    [Theory]
    [InlineData(MqttNodeDesignId.Validity)]
    [InlineData(MqttNodeDesignId.Type)]
    public void Declares_a_derived_envelope_child_as_predefined(string childId)
    {
        var metadata = RulesDeserializer.Deserialize("Mqtt.yaml");

        metadata.GetEnvelopeParents(childId).Should().AllSatisfy(parent =>
        {
            metadata.CanLinkInEnvelope(parent.Id, childId, DataPortTransferDirection.Outbound).Should().BeFalse();
            metadata.CanLinkInEnvelope(parent.Id, childId, DataPortTransferDirection.Inbound).Should().BeFalse();
        });
    }

    /// <summary>
    /// The type annotation is written as the assembly-qualified name of the parent value's runtime
    /// type, so the child declares the string it puts on the wire even though the port, and not the
    /// engine, supplies it.
    /// </summary>
    [Fact]
    public void Declares_the_type_child_as_the_string_it_transmits()
    {
        var metadata = RulesDeserializer.Deserialize("Mqtt.yaml");

        var type = metadata.NodeTypes
            .OfType<DataPortTreeNodeType>()
            .Single(n => n.Id == MqttNodeDesignId.Type);

        type.DataTypes.Should().Equal("String");
    }

    [Theory]
    [InlineData(MqttNodeDesignId.UserProperty)]
    [InlineData(MqttNodeDesignId.Timestamp)]
    [InlineData(MqttNodeDesignId.Validity)]
    [InlineData(MqttNodeDesignId.Type)]
    public void Declares_an_envelope_child_that_carries_a_value_of_its_own(string childId)
    {
        var metadata = RulesDeserializer.Deserialize("Mqtt.yaml");

        metadata.IsEnvelopeChild(childId).Should().BeTrue();
        metadata.IsValueNode(childId).Should().BeTrue();
        metadata.IsMarkerEnvelopeChild(childId).Should().BeFalse();
    }

    [Theory]
    [InlineData("batchId")]
    [InlineData("Type of value")]
    [InlineData("device/serial#1")]
    public void Accepts_a_user_property_key(string name)
        => ValidateNodeName(MqttNodeDesignId.UserProperty, name).Should().BeTrue();

    [Theory]
    [InlineData("")]
    [InlineData("batch\tId")]
    public void Rejects_an_unprintable_user_property_key(string name)
        => ValidateNodeName(MqttNodeDesignId.UserProperty, name).Should().BeFalse();

    private static bool ValidateNodeName(string nodeTypeId, string name)
    {
        var metadata = RulesDeserializer.Deserialize("Mqtt.yaml");

        var validations = metadata.NodeTypes
            .Single(n => n.Id == nodeTypeId)
            .NameValidations;

        validations.Should().NotBeNullOrEmpty();

        return validations.All(v => v.Validate(name).IsValid);
    }
}
