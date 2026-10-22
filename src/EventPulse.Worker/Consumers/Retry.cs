using System.Diagnostics;
using Microsoft.Extensions.Options;

namespace EventPulse.Worker.Consumers;

internal enum RetryResult
{
    Done,
    Exhausted,
    Expired,
    Stopped,
}

internal readonly record struct Outcome(RetryResult Result, int Attempts, Exception? Error);

internal sealed class Retry(IOptions<RetryOptions> options)
{
    private readonly RetryOptions _options = options.Value;

    public int Attempts => _options.Attempts;

    public TimeSpan MaxDelay => _options.MaxDelay;

    public TimeSpan Backoff(int failed) =>
        TimeSpan.FromMilliseconds(Math.Min(
            _options.BaseDelay.TotalMilliseconds * Math.Pow(2, failed - 1),
            _options.MaxDelay.TotalMilliseconds));

    // The deadline counts from when the message was consumed, so the whole message stays inside max.poll.interval.ms.
    public Outcome Run(
        Action<int> attempt,
        int limit,
        Func<int, TimeSpan> wait,
        Stopwatch since,
        Action<int, Exception, TimeSpan> retrying,
        CancellationToken token)
    {
        for (var n = 1; ; n++)
        {
            try
            {
                attempt(n);
                return new(RetryResult.Done, n, null);
            }
            catch (Exception ex)
            {
                if (n >= limit)
                {
                    return new(RetryResult.Exhausted, n, ex);
                }

                var delay = wait(n);
                if (since.Elapsed + delay > _options.Deadline)
                {
                    return new(RetryResult.Expired, n, ex);
                }

                retrying(n, ex, delay);
                if (token.WaitHandle.WaitOne(delay))
                {
                    return new(RetryResult.Stopped, n, ex);
                }
            }
        }
    }
}
