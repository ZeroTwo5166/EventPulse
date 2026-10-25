using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventPulse.Contracts;
using EventPulse.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;

namespace EventPulse.IntegrationTests;

public sealed class StackFixture : IAsyncLifetime
{
    private static readonly TimeSpan WorkerStartTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan WorkerStopTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

    private IHost? _worker;
    private Task? _workerRun;

    public KafkaContainer Kafka { get; } = new KafkaBuilder("confluentinc/cp-kafka:7.9.0")
        .WithKRaft()
        .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .Build();

    // A random host port would change on docker stop and start, while the Worker keeps the one it was started with.
    public PostgreSqlContainer Postgres { get; } = new PostgreSqlBuilder("postgres:16")
        .WithPortBinding(FreePort(), 5432)
        .Build();

    // Same reason as Postgres: the API's multiplexer reconnects to the port it was first given.
    public RedisContainer Redis { get; } = new RedisBuilder("redis:7")
        .WithPortBinding(FreePort(), 6379)
        .Build();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(Kafka.StartAsync(), Postgres.StartAsync(), Redis.StartAsync());

        using var admin = Admin();

        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = Topics.Events,
                NumPartitions = 6,
                ReplicationFactor = 1,
                Configs = new() { ["retention.ms"] = "604800000" },
            },
            new TopicSpecification
            {
                Name = Topics.DeadLetters,
                NumPartitions = 1,
                ReplicationFactor = 1,
                Configs = new() { ["retention.ms"] = "2592000000" },
            },
        ]);
    }

    public async Task DisposeAsync()
    {
        if (_worker is not null)
        {
            _worker.Services.GetRequiredService<IHostApplicationLifetime>().StopApplication();
        }

        try
        {
            if (_workerRun is not null)
            {
                await _workerRun.WaitAsync(WorkerStopTimeout);
            }
        }
        finally
        {
            await Task.WhenAll(Kafka.DisposeAsync().AsTask(), Postgres.DisposeAsync().AsTask(), Redis.DisposeAsync().AsTask());
        }
    }

    public async Task StartWorkerAsync()
    {
        if (_workerRun is not null)
        {
            return;
        }

        var root = WorkerContentRoot();
        var ledger = LedgerConnectionString(root);
        string[] args =
        [
            $"--contentRoot={root}",
            $"--ConnectionStrings:Ledger={ledger}",
            $"--Kafka:BootstrapServers={Kafka.GetBootstrapAddress()}",
        ];

        var capture = new HostCapture(ledger);
        using (DiagnosticListener.AllListeners.Subscribe(capture))
        {
            var entry = typeof(KafkaOptions).Assembly.EntryPoint!;
            _workerRun = Task.Run(() => entry.Invoke(null, [args]));
            await UnlessWorkerExits(capture.Host, "built its host");
        }

        _worker = await capture.Host;

        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = _worker.Services.GetRequiredService<IHostApplicationLifetime>()
            .ApplicationStarted.Register(() => started.TrySetResult());
        await UnlessWorkerExits(started.Task, "started");
    }

    // Returns how long StopAsync took. Program.cs disposes the host once it has stopped, so a later StartWorkerAsync builds a fresh one.
    public async Task<TimeSpan> StopWorkerAsync()
    {
        var elapsed = Stopwatch.StartNew();
        await _worker!.StopAsync();
        var took = elapsed.Elapsed;

        await _workerRun!.WaitAsync(WorkerStopTimeout);
        _worker = null;
        _workerRun = null;
        return took;
    }

    public T WorkerService<T>()
        where T : notnull => _worker!.Services.GetRequiredService<T>();

    public async Task WaitForProcessedAsync(IEnumerable<Guid> eventIds, TimeSpan timeout)
    {
        var ids = eventIds.Distinct().ToArray();
        await using var db = NpgsqlDataSource.Create(Postgres.GetConnectionString());
        var elapsed = Stopwatch.StartNew();

        while (true)
        {
            EnsureWorkerRunning();

            await using var cmd = db.CreateCommand("select count(*) from processed_events where event_id = any(@ids)");
            cmd.Parameters.AddWithValue("ids", ids);
            var seen = (long)(await cmd.ExecuteScalarAsync())!;

            if (seen == ids.Length)
            {
                return;
            }

            if (elapsed.Elapsed > timeout)
            {
                throw new TimeoutException($"{seen} of {ids.Length} events in processed_events after {timeout}");
            }

            await Task.Delay(PollInterval);
        }
    }

    public async Task WaitForWorkerCaughtUpAsync(TimeSpan timeout)
    {
        var options = _worker!.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;
        string[] groups = [options.GroupId, options.NotificationsGroupId];
        using var admin = Admin();
        var parts = admin.GetMetadata(Topics.Events, TimeSpan.FromSeconds(10)).Topics.Single().Partitions
            .Select(p => new TopicPartition(Topics.Events, p.PartitionId))
            .ToList();
        var elapsed = Stopwatch.StartNew();

        while (true)
        {
            EnsureWorkerRunning();

            var ends = await admin.ListOffsetsAsync(
                parts.Select(p => new TopicPartitionOffsetSpec { TopicPartition = p, OffsetSpec = OffsetSpec.Latest() }));
            var behind = new List<string>();
            foreach (var group in groups)
            {
                // librdkafka accepts only one group per ListConsumerGroupOffsets request.
                var committed = (await admin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(group, parts)]))
                    .Single().Partitions
                    .ToDictionary(p => p.TopicPartition, p => Math.Max(0, p.Offset.Value));
                var lag = ends.ResultInfos.Sum(r => r.TopicPartitionOffsetError.Offset.Value - committed[r.TopicPartitionOffsetError.TopicPartition]);
                if (lag > 0)
                {
                    behind.Add($"{group} {lag}");
                }
            }

            if (behind.Count == 0)
            {
                return;
            }

            if (elapsed.Elapsed > timeout)
            {
                throw new TimeoutException($"Messages behind after {timeout}: {string.Join(", ", behind)}");
            }

            await Task.Delay(PollInterval);
        }
    }

    private IAdminClient Admin() =>
        new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Kafka.GetBootstrapAddress() }).Build();

    private async Task UnlessWorkerExits(Task step, string stage)
    {
        if (await Task.WhenAny(step, _workerRun!).WaitAsync(WorkerStartTimeout) == step)
        {
            return;
        }

        await _workerRun!;
        throw new InvalidOperationException($"Worker exited before it {stage}");
    }

    private void EnsureWorkerRunning()
    {
        if (_workerRun is { IsCompleted: true })
        {
            throw new InvalidOperationException("Worker stopped; its log output has the failure", _workerRun.Exception);
        }
    }

    // Address and credentials come from the container; everything else, such as the timeouts, from the Worker's appsettings.json.
    private string LedgerConnectionString(string root)
    {
        var settings = new ConfigurationBuilder().SetBasePath(root).AddJsonFile("appsettings.json").Build();
        var container = new NpgsqlConnectionStringBuilder(Postgres.GetConnectionString());
        return new NpgsqlConnectionStringBuilder(settings.GetConnectionString("Ledger"))
        {
            Host = container.Host,
            Port = container.Port,
            Database = container.Database,
            Username = container.Username,
            Password = container.Password,
        }.ConnectionString;
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    // The API's appsettings.json is also copied into the test output, so read the Worker's from its project directory.
    private static string WorkerContentRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "EventPulse.sln")))
        {
            dir = dir.Parent;
        }

        return dir is null
            ? throw new DirectoryNotFoundException("EventPulse.sln not found above the test output")
            : Path.Combine(dir.FullName, "src", "EventPulse.Worker");
    }

    // Program.cs builds and runs its host in one statement; this is the hook WebApplicationFactory uses to get hold of it.
    // Other hosts can be built at the same time, and only ours has this connection string.
    private sealed class HostCapture(string ledger) : IObserver<DiagnosticListener>, IObserver<KeyValuePair<string, object?>>
    {
        private readonly TaskCompletionSource<IHost> _host = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<IHost> Host => _host.Task;

        public void OnNext(DiagnosticListener listener)
        {
            if (listener.Name == "Microsoft.Extensions.Hosting")
            {
                listener.Subscribe(this);
            }
        }

        public void OnNext(KeyValuePair<string, object?> e)
        {
            if (e.Key == "HostBuilt"
                && e.Value is IHost host
                && host.Services.GetRequiredService<IConfiguration>().GetConnectionString("Ledger") == ledger)
            {
                _host.TrySetResult(host);
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }
}
