using System.Diagnostics;
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
    Retry retry,
    IHostApplicationLifetime lifetime,
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

        var inFlight = 0;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                inFlight = 1;
                if (!Handle(result, stoppingToken))
                {
                    break;
                }

                consumer.Commit(result);
                if (!stoppingToken.IsCancellationRequested)
                {
                    inFlight = 0;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            Close(consumer, inFlight);
        }
    }

    private void Close(IConsumer<string, byte[]> consumer, int inFlight)
    {
        try
        {
            consumer.Close();
        }
        catch (KafkaException ex)
        {
            logger.LogWarning(ex, "Shutdown of {Group} with {InFlight} in flight, Close completed {Closed}", _options.NotificationsGroupId, inFlight, false);
            return;
        }

        logger.LogInformation("Shutdown of {Group} with {InFlight} in flight, Close completed {Closed}", _options.NotificationsGroupId, inFlight, true);
    }

    // There is no dead-letter path here, so past the schedule it keeps retrying at the cap until the deadline.
    private bool Handle(ConsumeResult<string, byte[]> result, CancellationToken stoppingToken)
    {
        var outcome = retry.Run(
            _ => Process(result),
            int.MaxValue,
            retry.Backoff,
            Stopwatch.StartNew(),
            (n, ex, delay) =>
            {
                var (_, eventId, eventType, orderId) = Incoming.Peek(result.Message.Value);
                logger.LogWarning(
                    "Attempt {Attempt} failed for {EventType} {EventId} for {OrderId} at {Partition}:{Offset}, retrying in {Delay}: {Error}",
                    n,
                    eventType,
                    eventId,
                    orderId ?? result.Message.Key,
                    result.Partition.Value,
                    result.Offset.Value,
                    delay,
                    ex.Message);
            },
            stoppingToken);

        if (outcome.Result == RetryResult.Done)
        {
            return true;
        }

        if (outcome.Result != RetryResult.Stopped)
        {
            var (_, eventId, eventType, orderId) = Incoming.Peek(result.Message.Value);
            logger.LogError(
                outcome.Error,
                "Deadline passed after {Attempts} attempts for {EventType} {EventId} for {OrderId} at {Partition}:{Offset}, stopping without committing",
                outcome.Attempts,
                eventType,
                eventId,
                orderId ?? result.Message.Key,
                result.Partition.Value,
                result.Offset.Value);
            lifetime.StopApplication();
        }

        return false;
    }

    private void Process(ConsumeResult<string, byte[]> result)
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

        Store(incoming.Envelope, result);
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
