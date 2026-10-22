using EventPulse.Worker;
using EventPulse.Worker.Consumers;
using EventPulse.Worker.Ledger;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;



var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.Section));
builder.Services.AddDbContext<LedgerDbContext>(o => o
    .UseNpgsql(builder.Configuration.GetConnectionString("Ledger"))
    // A duplicate delivery fails the processed_events insert by design; the consumer logs real failures itself.
    .ConfigureWarnings(w => w.Log(
        (RelationalEventId.CommandError, LogLevel.Information),
        (CoreEventId.SaveChangesFailed, LogLevel.Information))));
builder.Services.AddHostedService<OrderLedgerConsumer>();

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    await scope.ServiceProvider.GetRequiredService<LedgerDbContext>().Database.MigrateAsync();
}

await host.RunAsync();
