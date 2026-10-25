namespace EventPulse.Api;

public sealed class Counters
{
    private long _rateLimitBypassed;

    public long RateLimitBypassed => Interlocked.Read(ref _rateLimitBypassed);

    public void AddRateLimitBypassed() => Interlocked.Increment(ref _rateLimitBypassed);
}
