using EventPulse.Worker;
using EventPulse.Worker.Consumers;
using EventPulse.Worker.Ledger;
using Microsoft.EntityFrameworkCore;



var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.Section));
builder.Services.AddDbContext<LedgerDbContext>(o => o.UseNpgsql(builder.Configuration.GetConnectionString("Ledger")));
builder.Services.AddHostedService<OrderLedgerConsumer>();

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    await scope.ServiceProvider.GetRequiredService<LedgerDbContext>().Database.MigrateAsync();
}

await host.RunAsync();
