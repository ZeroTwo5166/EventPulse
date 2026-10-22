using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using EventPulse.Contracts;
using Microsoft.Extensions.Options;

namespace EventPulse.Api;

internal static class Endpoints
{
    private static readonly string[] KnownTypes = [nameof(OrderPlaced), nameof(PaymentCompleted), nameof(OrderCancelled)];

    public static async Task<IResult> PostEvent(
        HttpRequest request,
        IProducer<string, byte[]> producer,
        IOptions<KafkaOptions> options,
        ILogger<Program> logger)
    {
        EventEnvelope? envelope;
        try
        {
            envelope = await JsonSerializer.DeserializeAsync<EventEnvelope>(
                request.Body, EventJson.Options, request.HttpContext.RequestAborted);
        }
        catch (JsonException ex)
        {
            return Invalid(ex.Path is ['$', '.', ..] path ? path[2..] : "body", "is malformed");
        }

        if (envelope is null)
        {
            return Invalid("body", "is required");
        }

        if (Validate(envelope) is var (field, reason))
        {
            return Invalid(field, reason);
        }

        var message = new Message<string, byte[]>
        {
            Key = envelope.OrderId,
            Value = JsonSerializer.SerializeToUtf8Bytes(envelope, EventJson.Options),
            Headers = new Headers
            {
                { "eventType", Encoding.UTF8.GetBytes(envelope.EventType) },
                { "schemaVersion", Encoding.UTF8.GetBytes(envelope.SchemaVersion.ToString(CultureInfo.InvariantCulture)) },
            },
        };

        using var timeout = new CancellationTokenSource(options.Value.ProduceTimeout);
        try
        {
            await producer.ProduceAsync(Topics.Events, message, timeout.Token);
        }
        catch (Exception ex) when (ex is KafkaException or OperationCanceledException)
        {
            logger.LogWarning(ex, "Publish of event {EventId} failed", envelope.EventId);
            return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable, title: "Event log unavailable");
        }

        return Results.Accepted(value: new { envelope.EventId });
    }

    public static IResult Health(IAdminClient admin, IOptions<KafkaOptions> options)
    {
        try
        {
            admin.GetMetadata(Topics.Events, options.Value.HealthTimeout);
            return Results.Ok(new { kafka = "ok" });
        }
        catch (KafkaException)
        {
            return Results.Json(new { kafka = "unavailable" }, statusCode: StatusCodes.Status503ServiceUnavailable);
        }
    }

    private static (string Field, string Reason)? Validate(EventEnvelope e)
    {
        if (e.EventId == Guid.Empty)
        {
            return ("eventId", "is required");
        }

        if (!KnownTypes.Contains(e.EventType))
        {
            return ("eventType", $"must be one of {string.Join(", ", KnownTypes)}");
        }

        if (string.IsNullOrWhiteSpace(e.OrderId))
        {
            return ("orderId", "is required");
        }

        if (e.Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
        {
            return ("payload", "is required");
        }

        return null;
    }

    private static IResult Invalid(string field, string reason) =>
        Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid event",
            detail: $"{field} {reason}",
            extensions: new Dictionary<string, object?> { ["field"] = field });
}
