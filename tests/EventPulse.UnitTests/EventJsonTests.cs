using System.Text.Json;
using EventPulse.Contracts;

namespace EventPulse.UnitTests;

public class EventJsonTests
{
    private static readonly OrderPlaced Placed = new("cus_77", 24900, "DKK");

    private static EventEnvelope Envelope() => new(
        Guid.Parse("7c1f0b2e-4a0d-4d2f-9a3b-6f8b2a1c9e10"),
        nameof(OrderPlaced),
        "ord_10492",
        new DateTimeOffset(2026, 10, 25, 14, 3, 11, TimeSpan.Zero),
        1,
        JsonSerializer.SerializeToElement(Placed, EventJson.Options));

    [Fact]
    public void Envelope_with_order_placed_round_trips()
    {
        var envelope = Envelope();

        var json = JsonSerializer.Serialize(envelope, EventJson.Options);
        var back = JsonSerializer.Deserialize<EventEnvelope>(json, EventJson.Options)!;

        // JsonElement compares by reference, so swap it out and check the payload separately.
        Assert.Equal(envelope, back with { Payload = envelope.Payload });
        Assert.Equal(Placed, back.Payload.Deserialize<OrderPlaced>(EventJson.Options));
    }

    [Fact]
    public void Serialized_envelope_uses_camel_case_keys()
    {
        var json = JsonSerializer.Serialize(Envelope(), EventJson.Options);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal(
            ["eventId", "eventType", "orderId", "occurredAt", "schemaVersion", "payload"],
            root.EnumerateObject().Select(p => p.Name));
        Assert.Equal(
            ["customerId", "amountMinor", "currency"],
            root.GetProperty("payload").EnumerateObject().Select(p => p.Name));
    }
}
