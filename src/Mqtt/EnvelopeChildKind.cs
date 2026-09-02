namespace ViciOne.Suite.DataPort;

internal enum EnvelopeChildKind
{
    /// <summary>A freely named user property whose value the engine writes.</summary>
    UserProperty,

    /// <summary>
    /// The timestamp of the parent value outbound, and the timestamp the received message
    /// carries inbound. Linked inbound only: outbound the port always sends the one of the
    /// parent value.
    /// </summary>
    Timestamp,

    /// <summary>The engine validity of the parent value. Outbound only and never linked.</summary>
    Validity,

    /// <summary>
    /// The .NET type of the parent value, marking the message for a receiving ViciOne system to
    /// deserialize with the sender's type. Outbound only, and not linkable: the port writes the
    /// assembly-qualified name of the parent value's runtime type.
    /// </summary>
    Type,
}
