using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using EventPulse.Contracts;
using EventPulse.Worker;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit.Abstractions;

namespace EventPulse.IntegrationTests;

public sealed class RetryTests(StackFixture stack, ITestOutputHelper output) : IClassFixture<StackFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan Pause = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

    public Task InitializeAsync() => stack.StartWorkerAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Postgres_paused_briefly_is_retried_and_the_event_is_applied()
    {
        var elapsed = Stopwatch.StartNew();
        var counters = stack.WorkerService<Counters>();
        var retries = counters.Retries;
        // Both tests share one stack, so compare with the count before this test rather than with zero.
        var deadLetters = await CountDeadLetters();
        var orderId = NewOrderId();
        var placed = Event(orderId, new OrderPlaced("cus_77", 24900, "DKK"));

        await stack.Postgres.PauseAsync();
        try
        {
            await Produce(placed);
            await Task.Delay(Pause);
        }
        finally
        {
            await stack.Postgres.UnpauseAsync();
        }

        await stack.WaitForProcessedAsync([placed.EventId], Timeout);

        Assert.Equal("Placed", await ReadStatus(orderId));
        Assert.Equal(deadLetters, await CountDeadLetters());
        Assert.True(counters.Retries - retries >= 1, $"retries went from {retries} to {counters.Retries}");
        output.WriteLine($"Took {elapsed.Elapsed.TotalSeconds:F1} s, {counters.Retries - retries} retries");
    }

    [Fact]
    public async Task Postgres_stopped_past_the_schedule_dead_letters_and_the_partition_moves_on()
    {
        var elapsed = Stopwatch.StartNew();
        var counters = stack.WorkerService<Counters>();
        var attempts = stack.WorkerService<IOptions<RetryOptions>>().Value.Attempts;
        var retries = counters.Retries;
        var writeRetries = counters.DeadLetterWriteRetries;
        var orderId = NewOrderId();
        var placed = Event(orderId, new OrderPlaced("cus_77", 24900, "DKK"));

        await stack.Postgres.StopAsync();
        try
        {
            await Produce(placed);
            // A failed dead-letter write means every attempt of the schedule has already failed.
            await Until(() => counters.DeadLetterWriteRetries > writeRetries, "a dead-letter write retry");
        }
        finally
        {
            await stack.Postgres.StartAsync();
        }

        await stack.WaitForProcessedAsync([placed.EventId], Timeout);

        Assert.Null(await ReadStatus(orderId));
        var row = Assert.Single(await ReadDeadLetters(orderId));
        Assert.Equal(placed.EventId, row.EventId);
        Assert.Equal("RetriesExhausted", row.Reason);
        // The root cause varies (timeout, refused socket, server shutdown), so only the Npgsql head is fixed.
        Assert.Matches(@"^Npgsql\.\w*Exception: .+", row.Detail);
        Assert.Equal(attempts, row.Attempts);
        Assert.True(row.Published);
        Assert.Equal(attempts - 1, counters.Retries - retries);

        var next = NewOrderId();
        var nextPlaced = Event(next, new OrderPlaced("cus_88", 100, "EUR"));
        await Produce(nextPlaced);
        await stack.WaitForProcessedAsync([nextPlaced.EventId], Timeout);

        Assert.Equal("Placed", await ReadStatus(next));
        output.WriteLine(
            $"Took {elapsed.Elapsed.TotalSeconds:F1} s, {counters.Retries - retries} retries, "
            + $"{counters.DeadLetterWriteRetries - writeRetries} dead-letter write retries, detail: {row.Detail}");
    }

    private static string NewOrderId() => $"ord_{Guid.NewGuid():N}";

    private static EventEnvelope Event<T>(string orderId, T payload) =>
        new(
            Guid.NewGuid(),
            typeof(T).Name,
            orderId,
            DateTimeOffset.UtcNow,
            1,
            JsonSerializer.SerializeToElement(payload, EventJson.Options));

    private static async Task Until(Func<bool> condition, string what)
    {
        var elapsed = Stopwatch.StartNew();
        while (!condition())
        {
            if (elapsed.Elapsed > Timeout)
            {
                throw new TimeoutException($"No {what} after {Timeout}");
            }

            await Task.Delay(PollInterval);
        }
    }

    private async Task Produce(EventEnvelope e)
    {
        using var producer = new ProducerBuilder<string, byte[]>(
            new ProducerConfig { BootstrapServers = stack.Kafka.GetBootstrapAddress() }).Build();
        await producer.ProduceAsync(
            Topics.Events,
            new Message<string, byte[]> { Key = e.OrderId, Value = JsonSerializer.SerializeToUtf8Bytes(e, EventJson.Options) });
    }

    private async Task<string?> ReadStatus(string orderId)
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand("select status from orders where order_id = @id");
        cmd.Parameters.AddWithValue("id", orderId);
        return (string?)await cmd.ExecuteScalarAsync();
    }

    private async Task<long> CountDeadLetters()
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand("select count(*) from dead_letters");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<List<DeadLetterRow>> ReadDeadLetters(string orderId)
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand(
            """
            select event_id, reason, detail, attempts, dlq_published_at is not null
            from dead_letters where order_id = @id order by id
            """);
        cmd.Parameters.AddWithValue("id", orderId);
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<DeadLetterRow>();
        while (await reader.ReadAsync())
        {
            rows.Add(new DeadLetterRow(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt32(3),
                reader.GetBoolean(4)));
        }

        return rows;
    }

    private sealed record DeadLetterRow(Guid EventId, string Reason, string Detail, int Attempts, bool Published);
}
