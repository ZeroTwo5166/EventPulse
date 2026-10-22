using System.Text.Json;

namespace EventPulse.Contracts;

public sealed record EventEnvelope(
    Guid EventId,
    string EventType,
    string OrderId,
    DateTimeOffset OccurredAt,
    int SchemaVersion,
    JsonElement Payload);
