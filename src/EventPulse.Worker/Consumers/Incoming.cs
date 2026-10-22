using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using EventPulse.Contracts;

namespace EventPulse.Worker.Consumers;

internal sealed record Incoming(EventEnvelope Envelope, object Payload)
{
    private static readonly Dictionary<string, Type> PayloadTypes = new()
    {
        [nameof(OrderPlaced)] = typeof(OrderPlaced),
        [nameof(PaymentCompleted)] = typeof(PaymentCompleted),
        [nameof(OrderCancelled)] = typeof(OrderCancelled),
    };

    public static bool TryRead(
        byte[]? body,
        [NotNullWhen(true)] out Incoming? incoming,
        [NotNullWhen(false)] out string? malformed)
    {
        incoming = null;
        var e = Deserialize(body);
        if (e is null)
        {
            malformed = "InvalidEnvelope";
            return false;
        }

        malformed = Validate(e);
        if (malformed is not null)
        {
            return false;
        }

        if (ReadPayload(e) is not { } payload)
        {
            malformed = "InvalidPayload";
            return false;
        }

        incoming = new Incoming(e, payload);
        return true;
    }

    // Reads what it can from a message that may not be an envelope at all; a null field is one that could not be read.
    public static (string Raw, Guid? EventId, string? EventType, string? OrderId) Peek(byte[]? body)
    {
        body ??= [];
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(body);
        }
        catch (JsonException)
        {
            // raw_message is jsonb, so a body that is not JSON is kept as a JSON string of its text.
            return (JsonSerializer.Serialize(Encoding.UTF8.GetString(body)), null, null, null);
        }

        using (doc)
        {
            var root = doc.RootElement;
            Guid? eventId = Field(root, "eventId") is { } v && v.TryGetGuid(out var id) && id != Guid.Empty ? id : null;
            return (Encoding.UTF8.GetString(body), eventId, Text(root, "eventType"), Text(root, "orderId"));
        }
    }

    private static EventEnvelope? Deserialize(byte[]? body)
    {
        try
        {
            return JsonSerializer.Deserialize<EventEnvelope>(body ?? [], EventJson.Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string? Validate(EventEnvelope e) =>
        e.EventId == Guid.Empty ? "MissingEventId"
        : e.EventType is null || !PayloadTypes.ContainsKey(e.EventType) ? "UnknownEventType"
        : string.IsNullOrWhiteSpace(e.OrderId) ? "MissingOrderId"
        : null;

    private static object? ReadPayload(EventEnvelope e)
    {
        if (e.Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
        {
            return null;
        }

        object? payload;
        try
        {
            payload = e.Payload.Deserialize(PayloadTypes[e.EventType], EventJson.Options);
        }
        catch (JsonException)
        {
            return null;
        }

        // Fields the orders table would otherwise reject, which would look like a database outage instead of bad input.
        return payload is OrderPlaced placed
            && (string.IsNullOrWhiteSpace(placed.CustomerId) || placed.Currency is not { Length: 3 })
            ? null
            : payload;
    }

    private static JsonElement? Field(JsonElement root, string name) =>
        root.ValueKind == JsonValueKind.Object && root.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String
            ? v
            : null;

    private static string? Text(JsonElement root, string name) =>
        Field(root, name)?.GetString() is { } s && !string.IsNullOrWhiteSpace(s) ? s : null;
}
