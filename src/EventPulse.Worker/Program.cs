using Confluent.Kafka;
using EventPulse.Worker;
using EventPulse.Worker.Consumers;
using EventPulse.Worker.Ledger;
using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

// Host:ServicesStopConcurrently stops both consumers at once, so the shutdown budget is one message plus Close(), not two of each.
builder.Services.Configure<HostOptions>(builder.Configuration.GetSection("Host"));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.Section));
builder.Services.Configure<RetryOptions>(builder.Configuration.GetSection(RetryOptions.Section));
builder.Services.AddDbContext<LedgerDbContext>(o => o
    .UseNpgsql(builder.Configuration.GetConnectionString("Ledger"))
    // A duplicate delivery fails the processed_events insert by design; the consumer logs real failures itself.
    .ConfigureWarnings(w => w.Log(
        (RelationalEventId.CommandError, LogLevel.Information),
        (CoreEventId.SaveChangesFailed, LogLevel.Information))));
builder.Services.AddSingleton(sp =>
{
    var kafka = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    var config = new ProducerConfig
    {
        BootstrapServers = kafka.BootstrapServers,
        Acks = kafka.Acks,
        EnableIdempotence = kafka.EnableIdempotence,
        MessageSendMaxRetries = kafka.MessageSendMaxRetries,
        MessageTimeoutMs = (int)kafka.ProduceTimeout.TotalMilliseconds,
    };
    return new ProducerBuilder<string, byte[]>(config).Build();
});
builder.Services.AddSingleton(sp =>
    new DependentAdminClientBuilder(sp.GetRequiredService<IProducer<string, byte[]>>().Handle).Build());
builder.Services.AddSingleton<Counters>();
builder.Services.AddSingleton<Retry>();
builder.Services.AddHostedService<OrderLedgerConsumer>();
builder.Services.AddHostedService<OrderNotificationsConsumer>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    await scope.ServiceProvider.GetRequiredService<LedgerDbContext>().Database.MigrateAsync();
}

app.MapGet("/health", Endpoints.Health);
app.MapGet("/metrics", Endpoints.Metrics);

await app.RunAsync();
