using Confluent.Kafka;
using EventPulse.Contracts;
using EventPulse.Worker.Ledger;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;

namespace EventPulse.Worker.Consumers;

internal sealed class OrderNotificationsConsumer(
    IOptions<KafkaOptions> options,
    IServiceScopeFactory scopes,
    ILogger<OrderNotificationsConsumer> logger) : BackgroundService
{
    private readonly KafkaOptions _options = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
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
            GroupId = _options.NotificationsGroupId,
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
            if (!Incoming.TryRead(result.Message.Value, out var incoming, out var malformed))
            {
                // The order-ledger group records the dead letter; this group only has to get past it.
                logger.LogWarning(
                    "Malformed message at {Partition}:{Offset} skipped: {Detail}",
                    result.Partition.Value,
                    result.Offset.Value,
                    malformed);
                return;
            }

            envelope = incoming.Envelope;
            Store(envelope, result);
        }
        catch (Exception ex)
        {
            // EF logs its own failures at Information, so this is the only Error-level record with the offset.
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

        db.Notifications.Add(new Notification
        {
            EventId = envelope.EventId,
            OrderId = envelope.OrderId,
            Message = $"{envelope.EventType} for {envelope.OrderId}",
            CreatedAt = DateTimeOffset.UtcNow,
        });

        try
        {
            db.SaveChanges();
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
        {
            logger.LogInformation("Duplicate {EventId} for {OrderId}, skipped", envelope.EventId, envelope.OrderId);
            return;
        }

        logger.LogInformation(
            "Notified {EventType} {EventId} for {OrderId} at {Partition}:{Offset}",
            envelope.EventType,
            envelope.EventId,
            envelope.OrderId,
            result.Partition.Value,
            result.Offset.Value);
    }
}
