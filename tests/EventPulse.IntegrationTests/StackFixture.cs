using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventPulse.Contracts;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;

namespace EventPulse.IntegrationTests;

public sealed class StackFixture : IAsyncLifetime
{
    public KafkaContainer Kafka { get; } = new KafkaBuilder("confluentinc/cp-kafka:7.9.0")
        .WithKRaft()
        .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .Build();

    public PostgreSqlContainer Postgres { get; } = new PostgreSqlBuilder("postgres:16").Build();

    public RedisContainer Redis { get; } = new RedisBuilder("redis:7").Build();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(Kafka.StartAsync(), Postgres.StartAsync(), Redis.StartAsync());

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Kafka.GetBootstrapAddress() })
            .Build();

        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = Topics.Events,
                NumPartitions = 6,
                ReplicationFactor = 1,
                Configs = new() { ["retention.ms"] = "604800000" },
            },
            new TopicSpecification
            {
                Name = Topics.DeadLetters,
                NumPartitions = 1,
                ReplicationFactor = 1,
                Configs = new() { ["retention.ms"] = "2592000000" },
            },
        ]);
    }

    public async Task DisposeAsync()
    {
        await Task.WhenAll(Kafka.DisposeAsync().AsTask(), Postgres.DisposeAsync().AsTask(), Redis.DisposeAsync().AsTask());
    }
}
