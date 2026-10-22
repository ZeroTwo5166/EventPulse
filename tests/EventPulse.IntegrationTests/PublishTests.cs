using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using EventPulse.Contracts;
using Microsoft.AspNetCore.Mvc.Testing;

namespace EventPulse.IntegrationTests;

public sealed class PublishTests(StackFixture stack) : IClassFixture<StackFixture>
{
    private static readonly OrderPlaced Placed = new("cus_77", 24900, "DKK");

    [Fact]
    public async Task Posted_event_lands_on_orders_events_keyed_by_order()
    {
        await using var factory = new WebApplicationFactory<Program>();
        var api = factory.WithWebHostBuilder(b => b.UseSetting("Kafka:BootstrapServers", stack.Kafka.GetBootstrapAddress()));
        using var client = api.CreateClient();

        var posted = new EventEnvelope(
            Guid.NewGuid(),
            nameof(OrderPlaced),
            "ord_10492",
            new DateTimeOffset(2026, 10, 25, 14, 3, 11, TimeSpan.Zero),
            1,
            JsonSerializer.SerializeToElement(Placed, EventJson.Options));

        using var response = await client.PostAsJsonAsync("/events", posted, EventJson.Options);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = stack.Kafka.GetBootstrapAddress(),
            GroupId = $"publish-tests-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe(Topics.Events);

        var result = consumer.Consume(TimeSpan.FromSeconds(30));
        consumer.Close();

        Assert.NotNull(result);
        Assert.Equal(posted.OrderId, result.Message.Key);
        Assert.Equal(nameof(OrderPlaced), Encoding.UTF8.GetString(result.Message.Headers.GetLastBytes("eventType")));

        var consumed = JsonSerializer.Deserialize<EventEnvelope>(result.Message.Value, EventJson.Options)!;

        // JsonElement compares by reference, so swap it out and check the payload separately.
        Assert.Equal(posted, consumed with { Payload = posted.Payload });
        Assert.Equal(Placed, consumed.Payload.Deserialize<OrderPlaced>(EventJson.Options));
    }
}
