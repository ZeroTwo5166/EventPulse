namespace EventPulse.Worker.Ledger;

internal sealed class ProcessedEvent
{
    public Guid EventId { get; set; }
    public required string ConsumerGroup { get; set; }
    public DateTimeOffset ProcessedAt { get; set; }
}
