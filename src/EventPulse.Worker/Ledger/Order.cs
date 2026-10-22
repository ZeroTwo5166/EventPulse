namespace EventPulse.Worker.Ledger;

internal sealed class Order
{
    public required string OrderId { get; set; }
    public required string CustomerId { get; set; }
    public long AmountMinor { get; set; }
    public required string Currency { get; set; }
    public required string Status { get; set; }
    public int Version { get; set; } = 1;
    public DateTimeOffset UpdatedAt { get; set; }
}
