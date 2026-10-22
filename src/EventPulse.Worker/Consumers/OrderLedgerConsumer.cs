using System.Globalization;
using System.Text;
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
    IProducer<string, byte[]> producer,
    Counters counters,
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
        Incoming? incoming = null;
        try
        {
            if (!Incoming.TryRead(result.Message.Value, out incoming, out var malformed))
            {
                DeadLetter(result, "Malformed", malformed);
            }
            else if (Store(incoming, result) is { } conflict)
            {
                DeadLetter(result, "Conflict", conflict);
            }
        }
        catch (Exception ex)
        {
            // Until retry exists, a transient failure stops the worker with the offset uncommitted.
            logger.LogError(
                ex,
                "Failed {EventType} {EventId} for {OrderId} at {Partition}:{Offset}",
                incoming?.Envelope.EventType,
                incoming?.Envelope.EventId,
                incoming?.Envelope.OrderId ?? result.Message.Key,
                result.Partition.Value,
                result.Offset.Value);
            throw;
        }
    }

    private string? Store(Incoming incoming, ConsumeResult<string, byte[]> result)
    {
        var envelope = incoming.Envelope;
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
            counters.AddDuplicate();
            return null;
        }

        if (Apply(db, incoming, now) is { } conflict)
        {
            tx.Rollback();
            return conflict;
        }

        db.SaveChanges();
        tx.Commit();

        logger.LogInformation(
            "Applied {EventType} {EventId} for {OrderId} at {Partition}:{Offset}",
            envelope.EventType,
            envelope.EventId,
            envelope.OrderId,
            result.Partition.Value,
            result.Offset.Value);
        return null;
    }

    private static string? Apply(LedgerDbContext db, Incoming incoming, DateTimeOffset now)
    {
        var e = incoming.Envelope;
        if (incoming.Payload is OrderPlaced placed)
        {
            if (db.Orders.Find(e.OrderId) is not null)
            {
                return "DuplicateOrder";
            }

            db.Orders.Add(new Order
            {
                OrderId = e.OrderId,
                CustomerId = placed.CustomerId,
                AmountMinor = placed.AmountMinor,
                Currency = placed.Currency,
                Status = "Placed",
                UpdatedAt = now,
            });
            return null;
        }

        var next = incoming.Payload switch
        {
            PaymentCompleted => "Paid",
            OrderCancelled => "Cancelled",
            _ => throw new InvalidOperationException($"No ledger effect for {e.EventType}"),
        };

        var order = db.Orders.Find(e.OrderId);
        if (order is null)
        {
            return "OrderNotFound";
        }

        if (order.Status != "Placed")
        {
            return "InvalidTransition";
        }

        order.Status = next;
        order.Version++;
        order.UpdatedAt = now;
        return null;
    }

    private void DeadLetter(ConsumeResult<string, byte[]> result, string reason, string detail)
    {
        var (raw, eventId, eventType, orderId) = Incoming.Peek(result.Message.Value);
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
        var now = DateTimeOffset.UtcNow;
        var row = new DeadLetter
        {
            EventId = eventId,
            EventType = eventType,
            OrderId = orderId ?? (string.IsNullOrWhiteSpace(result.Message.Key) ? null : result.Message.Key),
            Reason = reason,
            Detail = detail,
            Attempts = 1,
            RawMessage = raw,
            FailedAt = now,
        };

        using (var tx = db.Database.BeginTransaction())
        {
            // Without a readable eventId there is nothing to deduplicate on, so a redelivery is dead-lettered again.
            if (eventId is { } id)
            {
                db.ProcessedEvents.Add(new ProcessedEvent { EventId = id, ConsumerGroup = _options.GroupId, ProcessedAt = now });
            }

            db.DeadLetters.Add(row);
            try
            {
                db.SaveChanges();
            }
            catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
            {
                tx.Rollback();
                logger.LogInformation("Duplicate {EventId} for {OrderId}, skipped", eventId, row.OrderId);
                counters.AddDuplicate();
                return;
            }

            tx.Commit();
        }

        Publish(result, row);
        row.DlqPublishedAt = DateTimeOffset.UtcNow;
        db.SaveChanges();

        logger.LogError(
            "Dead-lettered {EventType} {EventId} for {OrderId} at {Partition}:{Offset} as {Reason} {Detail}",
            row.EventType,
            row.EventId,
            row.OrderId,
            result.Partition.Value,
            result.Offset.Value,
            reason,
            detail);
        counters.AddDeadLettered();
    }

    private void Publish(ConsumeResult<string, byte[]> result, DeadLetter row)
    {
        var headers = new Headers();
        foreach (var h in result.Message.Headers ?? new Headers())
        {
            headers.Add(h.Key, h.GetValueBytes());
        }

        headers.Add("x-failure-reason", Encoding.UTF8.GetBytes(row.Reason));
        headers.Add("x-failure-detail", Encoding.UTF8.GetBytes(row.Detail));
        headers.Add("x-attempts", Encoding.UTF8.GetBytes(row.Attempts.ToString(CultureInfo.InvariantCulture)));
        headers.Add("x-source-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString(CultureInfo.InvariantCulture)));
        headers.Add("x-source-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString(CultureInfo.InvariantCulture)));

        var message = new Message<string, byte[]> { Key = result.Message.Key, Value = result.Message.Value, Headers = headers };
        producer.ProduceAsync(Topics.DeadLetters, message).GetAwaiter().GetResult();
    }
}
