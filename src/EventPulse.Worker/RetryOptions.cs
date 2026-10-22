namespace EventPulse.Worker;

public sealed class RetryOptions
{
    public const string Section = "Retry";

    public int Attempts { get; init; }
    public TimeSpan BaseDelay { get; init; }
    public TimeSpan MaxDelay { get; init; }
    public TimeSpan Deadline { get; init; }
}
