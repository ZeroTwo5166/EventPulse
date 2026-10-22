using System.Text.Json;
using System.Text.Json.Serialization;

namespace EventPulse.Contracts;

public static class EventJson
{
    public static JsonSerializerOptions Options { get; } = Create();

    private static JsonSerializerOptions Create()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Skip,
            WriteIndented = false,
        };
        // Shared across the whole process, so freeze it to stop any caller mutating it.
        options.MakeReadOnly(populateMissingResolver: true);
        return options;
    }
}
