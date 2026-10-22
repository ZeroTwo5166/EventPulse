using EventPulse.Worker;
using EventPulse.Worker.Consumers;



var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.Section));
builder.Services.AddHostedService<OrderLedgerConsumer>();
builder.Build().Run();
