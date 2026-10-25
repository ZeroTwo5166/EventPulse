using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Confluent.Kafka;
using EventPulse.Contracts;
using Microsoft.AspNetCore.Mvc.Testing;
using Npgsql;

namespace EventPulse.IntegrationTests;

public sealed class LedgerTests(StackFixture stack) : IClassFixture<StackFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private readonly WebApplicationFactory<Program> _factory = new();
    private HttpClient _api = null!;

    public async Task InitializeAsync()
    {
        await stack.StartWorkerAsync();
        _api = _factory
            .WithWebHostBuilder(b => b
                .UseSetting("Kafka:BootstrapServers", stack.Kafka.GetBootstrapAddress())
                .UseSetting("ConnectionStrings:Redis", stack.Redis.GetConnectionString()))
            .CreateClient();
        _api.DefaultRequestHeaders.Add("X-Api-Key", $"ledger-tests-{Guid.NewGuid():N}");
    }

    public async Task DisposeAsync()
    {
        _api.Dispose();
        await _factory.DisposeAsync();
    }

    [Fact]
    public async Task Duplicate_delivery_is_applied_once()
    {
        var placed = Event(NewOrderId(), new OrderPlaced("cus_77", 24900, "DKK"));

        await Post(placed);
        await Post(placed);

        await stack.WaitForProcessedAsync([placed.EventId], Timeout);
        // The redelivery leaves no row behind, so offsets are the only sign the Worker has read it.
        await stack.WaitForWorkerCaughtUpAsync(Timeout);

        Assert.Equal([new OrderRow(placed.OrderId, "Placed", 1)], await ReadOrders([placed.OrderId]));

        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand("select count(*) from processed_events where event_id = @id");
        cmd.Parameters.AddWithValue("id", placed.EventId);
        Assert.Equal(1L, await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Events_for_one_order_apply_in_order_from_one_partition()
    {
        var orderIds = Enumerable.Range(0, 20).Select(_ => NewOrderId()).ToArray();
        var placed = orderIds.Select(id => Event(id, new OrderPlaced("cus_77", 24900, "DKK"))).ToArray();
        var paid = orderIds.Select(id => Event(id, new PaymentCompleted($"pay_{id}", 24900))).ToArray();

        for (var i = 0; i <= orderIds.Length; i++)
        {
            if (i < orderIds.Length)
            {
                await Post(placed[i]);
            }

            if (i > 0)
            {
                await Post(paid[i - 1]);
            }
        }

        var eventIds = placed.Concat(paid).Select(e => e.EventId).ToHashSet();
        await stack.WaitForProcessedAsync(eventIds, Timeout);

        Assert.Equal(
            orderIds.Order(StringComparer.Ordinal).Select(id => new OrderRow(id, "Paid", 2)),
            (await ReadOrders(orderIds)).OrderBy(o => o.OrderId, StringComparer.Ordinal));

        var partitionOf = ReadPartitions(eventIds);
        Assert.Equal(eventIds.Count, partitionOf.Count);
        Assert.True(partitionOf.Values.Distinct().Count() > 1, "All 40 events landed on one partition");
        Assert.All(
            Enumerable.Range(0, orderIds.Length),
            i => Assert.Equal(partitionOf[placed[i].EventId], partitionOf[paid[i].EventId]));
    }

    [Fact]
    public async Task Duplicate_delivery_writes_one_notification()
    {
        var orderId = NewOrderId();
        var placed = Event(orderId, new OrderPlaced("cus_77", 24900, "DKK"));
        var paid = Event(orderId, new PaymentCompleted($"pay_{orderId}", 24900));

        await Post(placed);
        await Post(paid);
        await Post(placed);

        await stack.WaitForProcessedAsync([placed.EventId, paid.EventId], Timeout);
        await stack.WaitForWorkerCaughtUpAsync(Timeout);

        // order-notifications has committed past all three messages, so the third was consumed and left no row.
        Assert.Equal([placed.EventId, paid.EventId, placed.EventId], ReadEventIds(orderId, 3));
        Assert.Equal(
            [
                new NotificationRow(placed.EventId, $"OrderPlaced for {orderId}"),
                new NotificationRow(paid.EventId, $"PaymentCompleted for {orderId}"),
            ],
            await ReadNotifications(orderId));
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

    private async Task Post(EventEnvelope e)
    {
        using var response = await _api.PostAsJsonAsync("/events", e, EventJson.Options);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    private async Task<List<OrderRow>> ReadOrders(string[] orderIds)
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand("select order_id, status, version from orders where order_id = any(@ids)");
        cmd.Parameters.AddWithValue("ids", orderIds);
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<OrderRow>();
        while (await reader.ReadAsync())
        {
            rows.Add(new OrderRow(reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
        }

        return rows;
    }

    private Dictionary<Guid, int> ReadPartitions(IReadOnlySet<Guid> eventIds)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = stack.Kafka.GetBootstrapAddress(),
            GroupId = $"ledger-tests-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe(Topics.Events);

        var partitionOf = new Dictionary<Guid, int>();
        var deadline = DateTime.UtcNow + Timeout;
        while (partitionOf.Count < eventIds.Count && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result is null)
            {
                continue;
            }

            var e = JsonSerializer.Deserialize<EventEnvelope>(result.Message.Value, EventJson.Options)!;
            if (eventIds.Contains(e.EventId))
            {
                partitionOf[e.EventId] = result.Partition.Value;
            }
        }

        consumer.Close();
        return partitionOf;
    }

    private async Task<List<NotificationRow>> ReadNotifications(string orderId)
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand("select event_id, message from notifications where order_id = @id order by id");
        cmd.Parameters.AddWithValue("id", orderId);
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<NotificationRow>();
        while (await reader.ReadAsync())
        {
            rows.Add(new NotificationRow(reader.GetGuid(0), reader.GetString(1)));
        }

        return rows;
    }

    private List<Guid> ReadEventIds(string orderId, int expected)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = stack.Kafka.GetBootstrapAddress(),
            GroupId = $"ledger-tests-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe(Topics.Events);

        var eventIds = new List<Guid>();
        var deadline = DateTime.UtcNow + Timeout;
        while (eventIds.Count < expected && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result is null || result.Message.Key != orderId)
            {
                continue;
            }

            eventIds.Add(JsonSerializer.Deserialize<EventEnvelope>(result.Message.Value, EventJson.Options)!.EventId);
        }

        consumer.Close();
        return eventIds;
    }

    private sealed record OrderRow(string OrderId, string Status, int Version);

    private sealed record NotificationRow(Guid EventId, string Message);
}
