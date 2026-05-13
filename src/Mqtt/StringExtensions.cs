using System;
using ViciOne.ManagedEngine.TypeResolution;

namespace ViciOne.Suite.DataPort;

internal static class StringExtensions
{
    internal static Type ToType(this string value)
        => TypeResolver.GetType(value);
}
