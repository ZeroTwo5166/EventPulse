using Confluent.Kafka;
using EventPulse.Contracts;
using EventPulse.Worker.Ledger;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace EventPulse.Worker;

internal static class Endpoints
{
    public static async Task<IResult> Health(
        IAdminClient admin,
        LedgerDbContext db,
        IOptions<KafkaOptions> options,
        CancellationToken token)
    {
        var kafka = Reach(admin, options.Value.HealthTimeout);
        var postgres = await db.Database.CanConnectAsync(token);
        var body = new { kafka = kafka ? "ok" : "unavailable", postgres = postgres ? "ok" : "unavailable" };
        return kafka && postgres
            ? Results.Ok(body)
            : Results.Json(body, statusCode: StatusCodes.Status503ServiceUnavailable);
    }

    public static IResult Metrics(Counters counters) =>
        Results.Ok(new
        {
            processed = counters.Processed,
            duplicates = counters.Duplicates,
            retries = counters.Retries,
            dead_lettered = counters.DeadLettered,
            dead_letter_write_retries = counters.DeadLetterWriteRetries,
        });

    private static bool Reach(IAdminClient admin, TimeSpan timeout)
    {
        try
        {
            admin.GetMetadata(Topics.Events, timeout);
            return true;
        }
        catch (KafkaException)
        {
            return false;
        }
    }
}
