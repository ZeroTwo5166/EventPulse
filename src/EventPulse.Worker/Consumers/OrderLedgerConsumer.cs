using System.Text.Json;
using Confluent.Kafka;
using EventPulse.Contracts;
using EventPulse.Worker.Ledger;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;

namespace EventPulse.Worker.Consumers;

internal sealed class OrderLedgerConsumer(
    IOptions<KafkaOptions> options,
    IServiceScopeFactory scopes,
    ILogger<OrderLedgerConsumer> logger) : BackgroundService
{
    private readonly KafkaOptions _options = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Consume() blocks, so the loop gets its own thread instead of a thread-pool one.
        return Task.Factory.StartNew(
            () => Run(stoppingToken),
            stoppingToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    private void Run(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.GroupId,
            EnableAutoCommit = _options.EnableAutoCommit,
            AutoOffsetReset = _options.AutoOffsetReset,
            MaxPollIntervalMs = _options.MaxPollIntervalMs,
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetPartitionsAssignedHandler((_, parts) =>
                logger.LogInformation("Assigned {Partitions}", parts.Select(p => p.Partition.Value)))
            .SetPartitionsRevokedHandler((_, parts) =>
                logger.LogInformation("Revoked {Partitions}", parts.Select(p => p.Partition.Value)))
            .Build();

        consumer.Subscribe(Topics.Events);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                Handle(result);
                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown requested; fall through to Close.
        }
        finally
        {
            consumer.Close();
        }
    }

    private void Handle(ConsumeResult<string, byte[]> result)
    {
        EventEnvelope? envelope = null;
        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope>(result.Message.Value, EventJson.Options)
                ?? throw new JsonException("Message body is JSON null");
            Store(envelope, result);
        }
        catch (Exception ex)
        {
            // Until retry and dead-lettering exist, any failure stops the worker with the offset uncommitted.
            logger.LogError(
                ex,
                "Failed {EventType} {EventId} for {OrderId} at {Partition}:{Offset}",
                envelope?.EventType,
                envelope?.EventId,
                envelope?.OrderId ?? result.Message.Key,
                result.Partition.Value,
                result.Offset.Value);
            throw;
        }
    }

    private void Store(EventEnvelope envelope, ConsumeResult<string, byte[]> result)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
        using var tx = db.Database.BeginTransaction();
        var now = DateTimeOffset.UtcNow;

        db.ProcessedEvents.Add(new ProcessedEvent
        {
            EventId = envelope.EventId,
            ConsumerGroup = _options.GroupId,
            ProcessedAt = now,
        });

        // Flushed before Apply so a redelivery is caught here rather than failing Apply's status check.
        try
        {
            db.SaveChanges();
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
        {
            tx.Rollback();
            logger.LogInformation("Duplicate {EventId} for {OrderId}, skipped", envelope.EventId, envelope.OrderId);
            return;
        }

        Apply(db, envelope, now);
        db.SaveChanges();
        tx.Commit();

        logger.LogInformation(
            "Applied {EventType} {EventId} for {OrderId} at {Partition}:{Offset}",
            envelope.EventType,
            envelope.EventId,
            envelope.OrderId,
            result.Partition.Value,
            result.Offset.Value);
    }

    private static void Apply(LedgerDbContext db, EventEnvelope e, DateTimeOffset now)
    {
        if (e.EventType == nameof(OrderPlaced))
        {
            var placed = e.Payload.Deserialize<OrderPlaced>(EventJson.Options)
                ?? throw new JsonException("OrderPlaced payload is JSON null");
            db.Orders.Add(new Order
            {
                OrderId = e.OrderId,
                CustomerId = placed.CustomerId,
                AmountMinor = placed.AmountMinor,
                Currency = placed.Currency,
                Status = "Placed",
                UpdatedAt = now,
            });
            return;
        }

        var next = e.EventType switch
        {
            nameof(PaymentCompleted) => "Paid",
            nameof(OrderCancelled) => "Cancelled",
            _ => throw new InvalidOperationException($"Unknown event type {e.EventType}"),
        };

        var order = db.Orders.Find(e.OrderId)
            ?? throw new InvalidOperationException($"Order {e.OrderId} not found");

        if (order.Status != "Placed")
        {
            throw new InvalidOperationException($"{e.EventType} cannot move order {e.OrderId} from {order.Status} to {next}");
        }

        order.Status = next;
        order.Version++;
        order.UpdatedAt = now;
    }
}
