namespace EventPulse.Api;

public sealed class RateLimitOptions
{
    public const string Section = "RateLimit";

    public int Limit { get; init; }
    public TimeSpan Window { get; init; }
    public TimeSpan Timeout { get; init; }
}
