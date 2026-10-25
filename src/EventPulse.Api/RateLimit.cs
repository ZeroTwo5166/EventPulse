using System.Globalization;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventPulse.Api;

internal sealed class RateLimit(
    IConnectionMultiplexer redis,
    IOptions<RateLimitOptions> options,
    TimeProvider clock,
    Counters counters,
    ILogger<RateLimit> logger) : IEndpointFilter
{
    public const string Header = "X-Api-Key";

    // One script, so a dropped connection between INCR and EXPIRE cannot leave a counter that never expires.
    private static readonly string Script = """
        local n = redis.call('INCR', KEYS[1])
        if n == 1 then
            redis.call('EXPIRE', KEYS[1], ARGV[1])
        end
        return n
        """;

    private readonly RateLimitOptions _options = options.Value;

    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        var http = context.HttpContext;
        var apiKey = http.Request.Headers[Header].ToString();
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            return Endpoints.Invalid(Header, "is required");
        }

        var window = (long)_options.Window.TotalSeconds;
        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        var count = await Count($"ratelimit:{apiKey}:{now / window}", window);

        if (count > _options.Limit)
        {
            http.Response.Headers.RetryAfter = (window - (now % window)).ToString(CultureInfo.InvariantCulture);
            return Results.Problem(statusCode: StatusCodes.Status429TooManyRequests, title: "Rate limit exceeded");
        }

        return await next(context);
    }

    // Null means the request goes through uncounted: the limit protects Kafka, it is not worth refusing traffic over.
    private async Task<long?> Count(string key, long window)
    {
        if (!redis.IsConnected)
        {
            Bypass(null, "disconnected");
            return null;
        }

        try
        {
            var n = await redis.GetDatabase()
                .ScriptEvaluateAsync(Script, [key], [window])
                .WaitAsync(_options.Timeout);
            return (long)n;
        }
        catch (TimeoutException ex)
        {
            Bypass(ex, $"slower than {_options.Timeout.TotalMilliseconds} ms");
            return null;
        }
        catch (RedisException ex)
        {
            Bypass(ex, "failed");
            return null;
        }
    }

    private void Bypass(Exception? ex, string reason)
    {
        counters.AddRateLimitBypassed();
        logger.LogWarning(ex, "ratelimit bypassed, Redis {Reason}", reason);
    }
}
