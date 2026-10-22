using System.Diagnostics;
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
    Retry retry,
    Counters counters,
    IHostApplicationLifetime lifetime,
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
                if (!Handle(result, stoppingToken))
                {
                    break;
                }

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

    // False means the offset must stay uncommitted and the loop must stop: shutdown, or the host is being stopped.
    private bool Handle(ConsumeResult<string, byte[]> result, CancellationToken stoppingToken)
    {
        var since = Stopwatch.StartNew();
        var processed = retry.Run(
            n => Process(result, n),
            retry.Attempts,
            retry.Backoff,
            since,
            (n, ex, delay) =>
            {
                counters.AddRetry();
                LogRetry(result, "Attempt", n, ex, delay);
            },
            stoppingToken);

        if (processed.Result == RetryResult.Done)
        {
            return true;
        }

        if (processed.Result == RetryResult.Stopped)
        {
            return false;
        }

        if (processed.Result == RetryResult.Expired)
        {
            return GiveUp(result, processed);
        }

        var ex = processed.Error!;
        var (_, eventId, eventType, orderId) = Incoming.Peek(result.Message.Value);
        logger.LogError(
            ex,
            "Retries exhausted after {Attempts} attempts for {EventType} {EventId} for {OrderId} at {Partition}:{Offset}",
            processed.Attempts,
            eventType,
            eventId,
            orderId ?? result.Message.Key,
            result.Partition.Value,
            result.Offset.Value);
        var detail = Describe(ex);
        var written = retry.Run(
            _ => DeadLetter(result, "RetriesExhausted", detail, processed.Attempts),
            int.MaxValue,
            _ => retry.MaxDelay,
            since,
            (n, error, delay) =>
            {
                counters.AddDeadLetterWriteRetry();
                LogRetry(result, "Dead-letter write", n, error, delay);
            },
            stoppingToken);

        return written.Result switch
        {
            RetryResult.Done => true,
            RetryResult.Stopped => false,
            _ => GiveUp(result, written),
        };
    }

    // EF's wrapper says only "likely a transient failure"; the Npgsql exception names the server, its root cause says why.
    private static string Describe(Exception ex)
    {
        var head = ex;
        var root = ex;
        for (var e = ex; e is not null; e = e.InnerException)
        {
            if (e is NpgsqlException)
            {
                head = e;
            }

            root = e;
        }

        var text = $"{head.GetType().FullName}: {head.Message}";
        return root == head ? text : $"{text} ---> {root.GetType().FullName}: {root.Message}";
    }

    private void Process(ConsumeResult<string, byte[]> result, int attempt)
    {
        if (!Incoming.TryRead(result.Message.Value, out var incoming, out var malformed))
        {
            DeadLetter(result, "Malformed", malformed, attempt);
        }
        else if (Store(incoming, result) is { } conflict)
        {
            DeadLetter(result, "Conflict", conflict, attempt);
        }
    }

    // Committing here would lose the event, so the host stops and the message is redelivered on restart.
    private bool GiveUp(ConsumeResult<string, byte[]> result, Outcome outcome)
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
        return false;
    }

    private void LogRetry(ConsumeResult<string, byte[]> result, string what, int attempt, Exception ex, TimeSpan delay)
    {
        var (_, eventId, eventType, orderId) = Incoming.Peek(result.Message.Value);
        logger.LogWarning(
            "{What} {Attempt} failed for {EventType} {EventId} for {OrderId} at {Partition}:{Offset}, retrying in {Delay}: {Error}",
            what,
            attempt,
            eventType,
            eventId,
            orderId ?? result.Message.Key,
            result.Partition.Value,
            result.Offset.Value,
            delay,
            ex.Message);
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

    private void DeadLetter(ConsumeResult<string, byte[]> result, string reason, string detail, int attempts)
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
            Attempts = attempts,
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
