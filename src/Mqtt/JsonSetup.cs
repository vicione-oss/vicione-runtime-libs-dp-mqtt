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
            // A payload is read by broker tools and subscribers, not embedded in HTML, so text is
            // written as it is and only what JSON itself cannot hold is escaped.
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        };
}
