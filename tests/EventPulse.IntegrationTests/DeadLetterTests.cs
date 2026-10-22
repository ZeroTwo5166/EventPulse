using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using EventPulse.Contracts;
using Npgsql;

namespace EventPulse.IntegrationTests;

public sealed class DeadLetterTests(StackFixture stack) : IClassFixture<StackFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    public Task InitializeAsync() => stack.StartWorkerAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Malformed_message_is_dead_lettered_and_the_partition_moves_on()
    {
        var orderId = NewOrderId();
        var poison = Encoding.UTF8.GetBytes($"not json {Guid.NewGuid()}");
        var placed = Event(orderId, new OrderPlaced("cus_77", 24900, "DKK"));

        var source = await Produce(orderId, poison);
        await Produce(orderId, JsonSerializer.SerializeToUtf8Bytes(placed, EventJson.Options));

        // Same key, same partition: the valid event is only reached after the poison message is fully handled.
        await stack.WaitForProcessedAsync([placed.EventId], Timeout);

        Assert.Equal("Placed", await ReadStatus(orderId));
        Assert.Equal(
            new DeadLetterRow(null, null, "Malformed", "InvalidEnvelope", 1, true),
            Assert.Single(await ReadDeadLetters(orderId)));

        var dlq = Assert.Single(ReadDeadLetterTopic(orderId));
        Assert.Equal(poison, dlq.Message.Value);
        Assert.Equal("Malformed", Header(dlq, "x-failure-reason"));
        Assert.Equal("InvalidEnvelope", Header(dlq, "x-failure-detail"));
        Assert.Equal("1", Header(dlq, "x-attempts"));
        Assert.Equal(source.Partition.Value.ToString(CultureInfo.InvariantCulture), Header(dlq, "x-source-partition"));
        Assert.Equal(source.Offset.Value.ToString(CultureInfo.InvariantCulture), Header(dlq, "x-source-offset"));
    }

    [Fact]
    public async Task Payment_for_unknown_order_is_dead_lettered_once()
    {
        var orderId = NewOrderId();
        var paid = Event(orderId, new PaymentCompleted($"pay_{orderId}", 24900));
        var body = JsonSerializer.SerializeToUtf8Bytes(paid, EventJson.Options);

        await Produce(orderId, body);
        await stack.WaitForProcessedAsync([paid.EventId], Timeout);

        await Produce(orderId, body);
        // The redelivery leaves no row behind, so offsets are the only sign the Worker has read it.
        await stack.WaitForWorkerCaughtUpAsync(Timeout);

        Assert.Null(await ReadStatus(orderId));
        Assert.Equal(
            new DeadLetterRow(paid.EventId, nameof(PaymentCompleted), "Conflict", "OrderNotFound", 1, true),
            Assert.Single(await ReadDeadLetters(orderId)));
    }

    [Fact]
    public async Task Second_order_placed_for_existing_order_is_dead_lettered_and_the_partition_moves_on()
    {
        var orderId = NewOrderId();
        var placed = Event(orderId, new OrderPlaced("cus_77", 24900, "DKK"));
        var again = Event(orderId, new OrderPlaced("cus_88", 100, "EUR"));
        var paid = Event(orderId, new PaymentCompleted($"pay_{orderId}", 24900));

        await Produce(orderId, JsonSerializer.SerializeToUtf8Bytes(placed, EventJson.Options));
        await Produce(orderId, JsonSerializer.SerializeToUtf8Bytes(again, EventJson.Options));
        await Produce(orderId, JsonSerializer.SerializeToUtf8Bytes(paid, EventJson.Options));

        await stack.WaitForProcessedAsync([placed.EventId, again.EventId, paid.EventId], Timeout);

        Assert.Equal("Paid", await ReadStatus(orderId));
        Assert.Equal(
            new DeadLetterRow(again.EventId, nameof(OrderPlaced), "Conflict", "DuplicateOrder", 1, true),
            Assert.Single(await ReadDeadLetters(orderId)));
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

    private static string Header(ConsumeResult<string, byte[]> result, string key) =>
        Encoding.UTF8.GetString(result.Message.Headers.GetLastBytes(key));

    private async Task<DeliveryResult<string, byte[]>> Produce(string key, byte[] value)
    {
        using var producer = new ProducerBuilder<string, byte[]>(
            new ProducerConfig { BootstrapServers = stack.Kafka.GetBootstrapAddress() }).Build();
        return await producer.ProduceAsync(Topics.Events, new Message<string, byte[]> { Key = key, Value = value });
    }

    private async Task<string?> ReadStatus(string orderId)
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand("select status from orders where order_id = @id");
        cmd.Parameters.AddWithValue("id", orderId);
        return (string?)await cmd.ExecuteScalarAsync();
    }

    private async Task<List<DeadLetterRow>> ReadDeadLetters(string orderId)
    {
        await using var db = NpgsqlDataSource.Create(stack.Postgres.GetConnectionString());
        await using var cmd = db.CreateCommand(
            """
            select event_id, event_type, reason, detail, attempts, dlq_published_at is not null
            from dead_letters where order_id = @id order by id
            """);
        cmd.Parameters.AddWithValue("id", orderId);
        await using var reader = await cmd.ExecuteReaderAsync();

        var rows = new List<DeadLetterRow>();
        while (await reader.ReadAsync())
        {
            rows.Add(new DeadLetterRow(
                reader.IsDBNull(0) ? null : reader.GetGuid(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetInt32(4),
                reader.GetBoolean(5)));
        }

        return rows;
    }

    private List<ConsumeResult<string, byte[]>> ReadDeadLetterTopic(string orderId)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = stack.Kafka.GetBootstrapAddress(),
            GroupId = $"dead-letter-tests-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = true,
        }).Build();
        consumer.Subscribe(Topics.DeadLetters);

        var found = new List<ConsumeResult<string, byte[]>>();
        var deadline = DateTime.UtcNow + Timeout;
        while (DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result is null)
            {
                continue;
            }

            if (result.IsPartitionEOF)
            {
                break;
            }

            if (result.Message.Key == orderId)
            {
                found.Add(result);
            }
        }

        consumer.Close();
        return found;
    }

    private sealed record DeadLetterRow(Guid? EventId, string? EventType, string Reason, string Detail, int Attempts, bool Published);
}
