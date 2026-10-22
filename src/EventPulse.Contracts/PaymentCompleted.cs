namespace EventPulse.Contracts;

public sealed record PaymentCompleted(string PaymentId, long AmountMinor);
