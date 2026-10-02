using System.Runtime.Loader;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ViciOne.Suite.DataPort;

internal static class JsonSetup
{
    internal static JsonSerializerOptions PayloadOptions { get; } = CreatePayloadOptions();

    internal static JsonSerializerOptions CreatePayloadOptions(AssemblyLoadContext? assemblyLoadContext = default)
        => new()
        {
            Converters =
            {
                new JsonStringEnumConverter(),
                new TypeJsonConverter(assemblyLoadContext),
                new TypeNameHandlingConfig(assemblyLoadContext),
                new RangeCheckedFloatJsonConverter<double>(),
                new RangeCheckedFloatJsonConverter<float>(),
                new UtcDateTimeJsonConverter(),
            },
            // A payload is read by broker tools and subscribers, not embedded in HTML, so the
            // characters HTML needs escaped are written as they are, and so are letters and symbols
            // beyond ASCII.
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        };
}
