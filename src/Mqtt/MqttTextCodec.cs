using System;
using System.Globalization;
using System.Numerics;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// Converts a value between its runtime representation and the text an MQTT user property or a
/// plain text payload carries. Every conversion is culture invariant, so a message means the same
/// on the broker regardless of where it was produced.
/// </summary>
internal static class MqttTextCodec
{
    private const string TrueText = "true";
    private const string FalseText = "false";
    private const string RoundtripFormat = "O";

    /// <summary>
    /// What a timestamp is read from. The round-trip format this port writes demands exactly seven
    /// fractional-second digits, which no sender but this one has a reason to produce, so the
    /// fraction is optional here and may be one to seven digits or absent altogether. The zone is
    /// optional too, and may be <c>Z</c> or an offset. A text without a date and a time of day is
    /// not a point in time and is not read.
    /// </summary>
    private const string TimestampFormat = "yyyy-MM-ddTHH:mm:ss.FFFFFFFK";

    internal static string Format(object? value)
        => value switch
        {
            null => string.Empty,
            string text => text,
            bool flag => flag ? TrueText : FalseText,
            long number => number.ToString(CultureInfo.InvariantCulture),
            double number => number.ToString("R", CultureInfo.InvariantCulture),
            // A date of an unspecified kind means the UTC a received one is read as, not the local
            // time of whichever host sends it.
            DateTime { Kind: DateTimeKind.Unspecified } timestamp => FormatTimestamp(DateTime.SpecifyKind(timestamp, DateTimeKind.Utc)),
            DateTime timestamp => FormatTimestamp(timestamp),
            // The engine may deliver a narrower primitive than the tree declares, for instance an
            // int for an Int64 child.
            IConvertible convertible => convertible.ToString(CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };

    internal static object? Parse(string text, Type? type)
    {
        if (type == typeof(string))
            return text;
        if (type == typeof(bool))
            return bool.TryParse(text, out var flag) ? flag : null;
        // A sender that leaves the zone off means the UTC this port writes, not the local time of
        // whichever host happens to read the message.
        if (type == typeof(DateTime))
            return DateTime.TryParseExact(text, TimestampFormat, CultureInfo.InvariantCulture, DateTimeStyles.AllowLeadingWhite | DateTimeStyles.AllowTrailingWhite | DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var timestamp) ? timestamp : null;

        return Type.GetTypeCode(type) switch
        {
            TypeCode.SByte => ParseInteger<sbyte>(text),
            TypeCode.Byte => ParseInteger<byte>(text),
            TypeCode.Int16 => ParseInteger<short>(text),
            TypeCode.UInt16 => ParseInteger<ushort>(text),
            TypeCode.Int32 => ParseInteger<int>(text),
            TypeCode.UInt32 => ParseInteger<uint>(text),
            TypeCode.Int64 => ParseInteger<long>(text),
            TypeCode.UInt64 => ParseInteger<ulong>(text),
            TypeCode.Single => ParseFloat<float>(text),
            TypeCode.Double => ParseFloat<double>(text),
            _ => null,
        };
    }

    private static object? ParseInteger<T>(string text)
        where T : IBinaryInteger<T>
        => T.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) ? number : null;

    /// <summary>
    /// Reads a float, but not one too large for its type. The framework reads such a number as
    /// infinity, so a value the sender meant to be finite would arrive as one that is not.
    /// <c>Infinity</c> written as a word is read, because it has no digits to overflow.
    /// </summary>
    private static object? ParseFloat<T>(string text)
        where T : IFloatingPointIeee754<T>
        => T.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var number)
            && !(T.IsInfinity(number) && text.AsSpan().ContainsAnyInRange('0', '9'))
            ? number
            : null;

    /// <summary>
    /// Formats an engine timestamp the way the fixed <c>Timestamp</c> user property has always been
    /// written, so a subscriber built against the previous format keeps reading it.
    /// </summary>
    internal static string FormatTimestamp(DateTime timestamp)
        => timestamp.ToUniversalTime().ToString(RoundtripFormat, CultureInfo.InvariantCulture);

    /// <summary>
    /// Writes the engine validity as it stands, so a subscriber reads the state the engine gave the
    /// value and not only whether it was valid. Everything but zero means valid.
    /// </summary>
    internal static string FormatValidity(int validity)
        => validity.ToString(CultureInfo.InvariantCulture);
}
