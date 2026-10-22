namespace EventPulse.Worker.Ledger;

internal sealed class DeadLetter
{
    public long Id { get; set; }
    public Guid? EventId { get; set; }
    public string? EventType { get; set; }
    public string? OrderId { get; set; }
    public required string Reason { get; set; }
    public required string Detail { get; set; }
    public int Attempts { get; set; }
    public required string RawMessage { get; set; }
    public DateTimeOffset FailedAt { get; set; }
    public DateTimeOffset? DlqPublishedAt { get; set; }
}
