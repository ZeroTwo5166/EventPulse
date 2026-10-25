using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using EventPulse.Api;
using EventPulse.Contracts;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace EventPulse.IntegrationTests;

public sealed class RateLimitTests(StackFixture stack) : IClassFixture<StackFixture>, IAsyncLifetime
{
    private static readonly int Limit = 100;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    // Five seconds into a ten-second window, so a burst cannot straddle a boundary and Retry-After is known.
    private static readonly DateTimeOffset MidWindow = new(2026, 10, 25, 14, 3, 15, TimeSpan.Zero);

    private readonly WebApplicationFactory<Program> _factory = new();
    private readonly LogCapture _logs = new();
    private WebApplicationFactory<Program> _app = null!;
    private HttpClient _api = null!;

    public async Task InitializeAsync()
    {
        _app = _factory.WithWebHostBuilder(b => b
            .UseSetting("Kafka:BootstrapServers", stack.Kafka.GetBootstrapAddress())
            .UseSetting("ConnectionStrings:Redis", stack.Redis.GetConnectionString())
            .ConfigureLogging(l => l.AddProvider(_logs))
            .ConfigureTestServices(s => s.AddSingleton<TimeProvider>(new FixedClock(MidWindow))));
        _api = _app.CreateClient();

        // Connects the producer and the multiplexer, so the bursts below measure the limit and not startup.
        Assert.Equal(HttpStatusCode.Accepted, await Post(NewKey()));
    }

    public async Task DisposeAsync()
    {
        _api.Dispose();
        await _factory.DisposeAsync();
    }

    [Fact]
    public async Task Request_over_the_limit_gets_429_with_the_seconds_left_in_the_window()
    {
        var key = NewKey();

        var codes = await Task.WhenAll(Enumerable.Range(0, Limit).Select(_ => Post(key)));
        Assert.All(codes, c => Assert.Equal(HttpStatusCode.Accepted, c));

        using var over = await Send(key);
        Assert.Equal(HttpStatusCode.TooManyRequests, over.StatusCode);
        Assert.Equal(TimeSpan.FromSeconds(5), over.Headers.RetryAfter?.Delta);
    }

    [Fact]
    public async Task Two_keys_in_parallel_each_get_their_own_limit()
    {
        var a = NewKey();
        var b = NewKey();

        var codes = await Task.WhenAll(Enumerable.Range(0, Limit).SelectMany(_ => new[] { Post(a), Post(b) }));
        Assert.All(codes, c => Assert.Equal(HttpStatusCode.Accepted, c));

        Assert.Equal(HttpStatusCode.TooManyRequests, await Post(a));
        Assert.Equal(HttpStatusCode.TooManyRequests, await Post(b));
    }

    [Fact]
    public async Task Request_without_an_api_key_gets_400()
    {
        using var response = await _api.PostAsJsonAsync("/events", Event(), EventJson.Options);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("X-Api-Key", problem.GetProperty("field").GetString());
        Assert.Equal("X-Api-Key is required", problem.GetProperty("detail").GetString());
    }

    [Fact]
    public async Task Redis_down_fails_open_and_limiting_resumes_when_it_returns()
    {
        var counters = _app.Services.GetRequiredService<Counters>();
        var redis = _app.Services.GetRequiredService<IConnectionMultiplexer>();
        var key = NewKey();

        await stack.Redis.StopAsync();
        try
        {
            var codes = await Task.WhenAll(Enumerable.Range(0, Limit + 1).Select(_ => Post(key)));
            Assert.All(codes, c => Assert.Equal(HttpStatusCode.Accepted, c));
            Assert.Equal(Limit + 1, counters.RateLimitBypassed);
            Assert.Contains(_logs.Entries, e => e.Level == LogLevel.Warning && e.Message.StartsWith("ratelimit bypassed", StringComparison.Ordinal));
        }
        finally
        {
            await stack.Redis.StartAsync();
        }

        await Until(() => redis.IsConnected, "Redis reconnect");
        var resumed = NewKey();

        var after = await Task.WhenAll(Enumerable.Range(0, Limit).Select(_ => Post(resumed)));
        Assert.All(after, c => Assert.Equal(HttpStatusCode.Accepted, c));
        Assert.Equal(HttpStatusCode.TooManyRequests, await Post(resumed));
        Assert.Equal(Limit + 1, counters.RateLimitBypassed);
    }

    private static string NewKey() => $"ratelimit-tests-{Guid.NewGuid():N}";

    private static EventEnvelope Event() =>
        new(
            Guid.NewGuid(),
            nameof(OrderPlaced),
            $"ord_{Guid.NewGuid():N}",
            DateTimeOffset.UtcNow,
            1,
            JsonSerializer.SerializeToElement(new OrderPlaced("cus_77", 24900, "DKK"), EventJson.Options));

    private async Task<HttpResponseMessage> Send(string key)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(Event(), options: EventJson.Options),
        };
        request.Headers.Add("X-Api-Key", key);
        return await _api.SendAsync(request);
    }

    private async Task<HttpStatusCode> Post(string key)
    {
        using var response = await Send(key);
        return response.StatusCode;
    }

    private static async Task Until(Func<bool> condition, string what)
    {
        var elapsed = Stopwatch.StartNew();
        while (!condition())
        {
            if (elapsed.Elapsed > Timeout)
            {
                throw new TimeoutException($"No {what} after {Timeout}");
            }

            await Task.Delay(PollInterval);
        }
    }

    private sealed class FixedClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private sealed class LogCapture : ILoggerProvider
    {
        public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Sink(Entries);

        public void Dispose()
        {
        }

        private sealed class Sink(ConcurrentQueue<(LogLevel Level, string Message)> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                entries.Enqueue((logLevel, formatter(state, exception)));
        }
    }
}
