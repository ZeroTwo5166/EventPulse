using Confluent.Kafka;
using EventPulse.Worker;
using EventPulse.Worker.Consumers;
using EventPulse.Worker.Ledger;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Options;



var builder = Host.CreateApplicationBuilder(args);

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
    };
    return new ProducerBuilder<string, byte[]>(config).Build();
});
builder.Services.AddSingleton<Counters>();
builder.Services.AddSingleton<Retry>();
builder.Services.AddHostedService<OrderLedgerConsumer>();
builder.Services.AddHostedService<OrderNotificationsConsumer>();

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    await scope.ServiceProvider.GetRequiredService<LedgerDbContext>().Database.MigrateAsync();
}

await host.RunAsync();
