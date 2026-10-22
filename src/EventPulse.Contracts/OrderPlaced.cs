namespace EventPulse.Contracts;

public sealed record OrderPlaced(string CustomerId, long AmountMinor, string Currency);
