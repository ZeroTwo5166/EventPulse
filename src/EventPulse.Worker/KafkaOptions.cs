using Confluent.Kafka;

namespace EventPulse.Worker;

public sealed class KafkaOptions
{
    public const string Section = "Kafka";

    public string BootstrapServers { get; init; } = "";
    public string GroupId { get; init; } = "";
    public string NotificationsGroupId { get; init; } = "";
    public bool EnableAutoCommit { get; init; }
    public AutoOffsetReset AutoOffsetReset { get; init; }
    public int MaxPollIntervalMs { get; init; }
    public Acks Acks { get; init; }
    public bool EnableIdempotence { get; init; }
    public int MessageSendMaxRetries { get; init; }
}
