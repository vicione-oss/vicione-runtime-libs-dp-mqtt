using System.Diagnostics.CodeAnalysis;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// A payload type a data point can declare, the concrete one a sender may narrow it to, and an
/// abstract one it may name but no value can ever have been.
/// </summary>
internal record Measurement
{
    public int Value { get; init; }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes",
    Justification = "Instantiated by the deserializer under test")]
internal sealed record DetailedMeasurement : Measurement
{
    public string Unit { get; init; } = string.Empty;
}

/// <summary>
/// The shape a data point declares once a data type may be a complex one: a base no value can have
/// been, whose concrete subtype the sender names.
/// </summary>
internal abstract record PartialMeasurement : Measurement;

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes",
    Justification = "Instantiated by the deserializer under test")]
internal sealed record CompletedMeasurement : PartialMeasurement
{
    public string Unit { get; init; } = string.Empty;
}
