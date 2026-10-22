using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using EventPulse.Contracts;
using EventPulse.Worker;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit.Abstractions;

namespace EventPulse.IntegrationTests;

public sealed class ShutdownTests(StackFixture stack, ITestOutputHelper output) : IClassFixture<StackFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(20);

    [Fact]
    public async Task Stop_mid_batch_finishes_and_commits_so_a_fresh_worker_neither_misses_nor_repeats()
    {
        var orderIds = Enumerable.Range(1, 25).Select(_ => $"ord_{Guid.NewGuid():N}").ToArray();
        var placed = orderIds.Select(id => Event(id, new OrderPlaced("cus_77", 24900, "DKK"))).ToArray();
        var settled = orderIds.Select((id, i) => i % 2 == 0
            ? Event(id, new PaymentCompleted($"pay_{id}", 24900))
            : Event(id, new OrderCancelled("customer_request"))).ToArray();
        var events = placed.Concat(settled).ToArray();
        var eventIds = events.Select(e => e.EventId).ToArray();

        // Everything is in the log before the Worker starts, so the window to stop in is the whole of processing.
        await Produce(events);
        await stack.StartWorkerAsync();

        var shutdownTimeout = stack.WorkerService<IOptions<HostOptions>>().Value.ShutdownTimeout;
        var consumers = stack.WorkerService<IEnumerable<IHostedService>>().OfType<BackgroundService>().ToList();

        var seen = await WaitForSomeProcessed(eventIds);
        var took = await stack.StopWorkerAsync();
        var atStop = await CountProcessed(eventIds);

        output.WriteLine($"Stopped after {seen} of {events.Length} processed; StopAsync took {took.TotalMilliseconds:F0} ms, {atStop} processed at stop");
        Assert.True(took < shutdownTimeout, $"StopAsync took {took}, shutdown timeout is {shutdownTimeout}");
        Assert.All(consumers, c => Assert.True(c.ExecuteTask!.IsCompletedSuccessfully, $"{c.GetType().Name} did not finish"));
        Assert.True(atStop < events.Length, "Every event was processed before the stop took effect, so this run did not test shutdown");

        await stack.StartWorkerAsync();
        await stack.WaitForWorkerCaughtUpAsync(Timeout);

        Assert.Equal(orderIds.Length, await CountAtVersion2(orderIds));
        Assert.Equal(0L, await CountDeadLetters());
        Assert.Equal(events.Length, await CountProcessed(eventIds));
        Assert.Equal(0L, stack.WorkerService<Counters>().Duplicates);
    }

    private static EventEnvelope Event<T>(string orderId, T payload) =>
        new(
            Guid.NewGuid(),
            typeof(T).Name,
            orderId,
            DateTimeOffset.UtcNow,
            1,
            JsonSerializer.SerializeToElement(payload, EventJson.Options));

    private async Task<long> WaitForSomeProcessed(Guid[] eventIds)
    {
        var elapsed = Stopwatch.StartNew();
        while (true)
        {
            var n = await CountProcessed(eventIds);
            if (n == eventIds.Length)
            {
                throw new InvalidOperationException("All events were processed before the stop, so this run did not test shutdown");
            }

            if (n > 0)
            {
                return n;
            }

            if (elapsed.Elapsed > Timeout)
            {
                throw new TimeoutException($"No events processed after {Timeout}");
            }

            await Task.Delay(PollInterval);
        }
    }

    private async Task Produce(IEnumerable<EventEnvelope> events)
    {
        using var producer = new ProducerBuilder<string, byte[]>(
            new ProducerConfig { BootstrapServers = stack.Kafka.GetBootstrapAddress() }).Build();
        foreach (var e in events)
        {
            await producer.ProduceAsync(
                Topics.Events,
                new Message<string, byte[]> { Key = e.OrderId, Value = JsonSerializer.SerializeToUtf8Bytes(e, EventJson.Options) });
        }
    }

    private async Task<long> CountProcessed(Guid[] eventIds)
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand(
            "select count(*) from processed_events where consumer_group = 'order-ledger' and event_id = any(@ids)");
        cmd.Parameters.AddWithValue("ids", eventIds);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<long> CountAtVersion2(string[] orderIds)
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand("select count(*) from orders where order_id = any(@ids) and version = 2");
        cmd.Parameters.AddWithValue("ids", orderIds);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<long> CountDeadLetters()
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand("select count(*) from dead_letters");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}
