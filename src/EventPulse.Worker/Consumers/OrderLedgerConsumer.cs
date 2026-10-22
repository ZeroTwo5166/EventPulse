using System.Text.Json;
using Confluent.Kafka;
using EventPulse.Contracts;
using Microsoft.Extensions.Options;

namespace EventPulse.Worker.Consumers;

internal sealed class OrderLedgerConsumer(
    IOptions<KafkaOptions> options,
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
        var envelope = JsonSerializer.Deserialize<EventEnvelope>(result.Message.Value, EventJson.Options);

        logger.LogInformation(
            "Consumed {EventType} {EventId} for {OrderId} at {Partition}:{Offset}",
            envelope?.EventType,
            envelope?.EventId,
            envelope?.OrderId,
            result.Partition.Value,
            result.Offset.Value);
    }
}