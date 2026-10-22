using Confluent.Kafka;

namespace EventPulse.Api;

public sealed class KafkaOptions
{
    public const string Section = "Kafka";

    public string BootstrapServers { get; init; } = "";
    public Acks Acks { get; init; }
    public bool EnableIdempotence { get; init; }
    public int MessageSendMaxRetries { get; init; }
    public TimeSpan ProduceTimeout { get; init; }
    public TimeSpan HealthTimeout { get; init; }
}
