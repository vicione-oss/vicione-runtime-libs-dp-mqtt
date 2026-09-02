namespace ViciOne.Suite.DataPort;

/// <summary>The outcome of reading the value of an envelope child from a received message.</summary>
internal enum EnvelopeReadResult
{
    /// <summary>The message carries no property for the child's key.</summary>
    Missing,

    /// <summary>The property is there but its text is not a value of the child's data type.</summary>
    Malformed,

    /// <summary>The value was read.</summary>
    Read,
}
