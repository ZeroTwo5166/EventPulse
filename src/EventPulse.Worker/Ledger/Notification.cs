namespace EventPulse.Worker.Ledger;

internal sealed class Notification
{
    public long Id { get; set; }
    public Guid EventId { get; set; }
    public required string OrderId { get; set; }
    public required string Message { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}
