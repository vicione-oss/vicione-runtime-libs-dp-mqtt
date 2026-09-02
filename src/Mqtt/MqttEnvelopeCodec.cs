using System;
using System.Globalization;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// Converts the value of an envelope child between its runtime representation and the text of an
/// MQTT user property. Every conversion is culture invariant, so a message means the same on the
/// broker regardless of where it was produced.
/// </summary>
internal static class MqttEnvelopeCodec
{
    private const string TrueText = "true";
    private const string FalseText = "false";
    private const string RoundtripFormat = "O";

    internal static string Format(object? value)
        => value switch
        {
            null => string.Empty,
            string text => text,
            bool flag => flag ? TrueText : FalseText,
            long number => number.ToString(CultureInfo.InvariantCulture),
            double number => number.ToString("R", CultureInfo.InvariantCulture),
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
        if (type == typeof(long))
            return long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) ? number : null;
        if (type == typeof(double))
            return double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var number) ? number : null;
        if (type == typeof(DateTime))
            return DateTime.TryParseExact(text, RoundtripFormat, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out var timestamp) ? timestamp : null;

        return null;
    }

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
