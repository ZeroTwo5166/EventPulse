using Confluent.Kafka;
using EventPulse.Api;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.Section));

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

app.MapPost("/events", Endpoints.PostEvent);
app.MapGet("/health", Endpoints.Health);

app.Run();
