using Confluent.Kafka;
using EventPulse.Api;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.Section));
builder.Services.Configure<RateLimitOptions>(builder.Configuration.GetSection(RateLimitOptions.Section));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<Counters>();

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var config = ConfigurationOptions.Parse(
        builder.Configuration.GetConnectionString("Redis") ?? throw new InvalidOperationException("ConnectionStrings:Redis is not set"));
    // The rate limit fails open, so the API must start while Redis is down and keep reconnecting in the background.
    config.AbortOnConnectFail = false;
    return ConnectionMultiplexer.Connect(config);
});

builder.Services.AddSingleton(sp =>
{
    var kafka = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    var config = new ProducerConfig
    {
        BootstrapServers = kafka.BootstrapServers,
        Acks = kafka.Acks,
        EnableIdempotence = kafka.EnableIdempotence,
        MessageSendMaxRetries = kafka.MessageSendMaxRetries,
        // The cancellation token on ProduceAsync only abandons the await; this makes librdkafka give up on the message too.
        MessageTimeoutMs = (int)kafka.ProduceTimeout.TotalMilliseconds,
    };
    return new ProducerBuilder<string, byte[]>(config).Build();
});

builder.Services.AddSingleton(sp =>
    new DependentAdminClientBuilder(sp.GetRequiredService<IProducer<string, byte[]>>().Handle).Build());

var app = builder.Build();

app.Lifetime.ApplicationStopped.Register(() =>
{
    var kafka = app.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;
    app.Services.GetRequiredService<IProducer<string, byte[]>>().Flush(kafka.ProduceTimeout);
});

app.MapPost("/events", Endpoints.PostEvent).AddEndpointFilter<RateLimit>();
app.MapGet("/health", Endpoints.Health);

app.Run();
