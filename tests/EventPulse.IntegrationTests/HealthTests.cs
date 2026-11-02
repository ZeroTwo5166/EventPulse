using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using EventPulse.Contracts;
using Microsoft.AspNetCore.Mvc.Testing;

namespace EventPulse.IntegrationTests;

public sealed class HealthTests(StackFixture stack) : IClassFixture<StackFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private readonly WebApplicationFactory<Program> _factory = new();
    private HttpClient _api = null!;
    private HttpClient _worker = null!;

    public async Task InitializeAsync()
    {
        await stack.StartWorkerAsync();
        _worker = new HttpClient { BaseAddress = new Uri(stack.WorkerUrl) };
        _api = _factory
            .WithWebHostBuilder(b => b
                .UseSetting("Kafka:BootstrapServers", stack.Kafka.GetBootstrapAddress())
                .UseSetting("ConnectionStrings:Redis", stack.Redis.GetConnectionString()))
            .CreateClient();
        _api.DefaultRequestHeaders.Add("X-Api-Key", $"health-tests-{Guid.NewGuid():N}");
    }

    public async Task DisposeAsync()
    {
        _api.Dispose();
        _worker.Dispose();
        await _factory.DisposeAsync();
    }

    [Fact]
    public async Task Api_health_reports_kafka_and_redis_ok()
    {
        using var response = await _api.GetAsync("/health");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("ok", body.GetProperty("kafka").GetString());
        Assert.Equal("ok", body.GetProperty("redis").GetString());
    }

    [Fact]
    public async Task Worker_metrics_count_one_processed_per_applied_event()
    {
        var before = await Metrics();
        Assert.Equal(
            ["dead_letter_write_retries", "dead_lettered", "duplicates", "processed", "retries"],
            before.EnumerateObject().Select(p => p.Name).Order(StringComparer.Ordinal));

        var placed = new EventEnvelope(
            Guid.NewGuid(),
            nameof(OrderPlaced),
            $"ord_{Guid.NewGuid():N}",
            DateTimeOffset.UtcNow,
            1,
            JsonSerializer.SerializeToElement(new OrderPlaced("cus_77", 24900, "DKK"), EventJson.Options));
        using (var response = await _api.PostAsJsonAsync("/events", placed, EventJson.Options))
        {
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        }

        await stack.WaitForProcessedAsync([placed.EventId], Timeout);
        // The counter moves after the database commit and before the offset commit, so caught up means counted.
        await stack.WaitForWorkerCaughtUpAsync(Timeout);

        var after = await Metrics();
        Assert.Equal(before.GetProperty("processed").GetInt64() + 1, after.GetProperty("processed").GetInt64());
    }

    private async Task<JsonElement> Metrics()
    {
        using var response = await _worker.GetAsync("/metrics");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return await response.Content.ReadFromJsonAsync<JsonElement>();
    }
}
