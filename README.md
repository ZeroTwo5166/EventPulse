# EventPulse

EventPulse is an event-processing pipeline for a fictional order flow with three event types: `OrderPlaced`, `PaymentCompleted` and `OrderCancelled`. An ASP.NET Core API accepts events over HTTP, rate-limits each client with Redis and publishes the events to Kafka. A .NET Worker consumes them and applies them to an order ledger in PostgreSQL. The full design is in [docs/design.md](docs/design.md).

## Architecture

```
                    rate-limit counters
                       Redis (redis:7)
                            ^
                            | INCR + EXPIRE (Lua)
                            |
Client --POST /events--> EventPulse.Api --publish--> Kafka: orders.events (6 partitions, key = orderId)
                         :8080                              |
                                                            | consume (groups order-ledger, order-notifications)
                                                            v
                                                     EventPulse.Worker --apply--> PostgreSQL
                                                     :8081 (internal)             orders, processed_events,
                                                            |                     notifications, dead_letters
                                                            | permanent failures
                                                            v
                                                     Kafka: orders.events.dlq (1 partition)
                                                     + row in dead_letters
```

The API never connects to PostgreSQL, and the Worker serves no business traffic; the only state they share is the Kafka log. The Worker runs two consumer groups: `order-ledger` applies events to the ledger and dead-letters failures, and `order-notifications` writes one row per event to `notifications`. Topics are created by the one-shot `kafka-init` container; broker auto-creation is off.

## Running it locally

Prerequisites:

- Docker with Docker Compose.
- .NET 8 SDK (all projects target `net8.0` through `Directory.Build.props`). Only needed to build and run the tests outside Docker; the compose stack builds the API and Worker images itself.
- bash and curl for the scripts in `scripts/` and `tests/chaos/` (Git Bash on Windows).

Start the stack:

```
cp .env.example .env        # then set POSTGRES_PASSWORD in .env
docker compose up -d --build
```

The API listens on `http://localhost:8080`. Kafka is on `localhost:29092`, PostgreSQL on `localhost:5432` and Redis on `localhost:6379`. The Worker applies its EF Core migrations on startup. Running the Worker with `dotnet run` outside Docker needs `PGPASSWORD` set in the shell, because the connection string in its `appsettings.json` has no password.

Send seed events and verify the ledger:

```
scripts/seed.sh                   # 500 orders, 1,000 events; prints the prefix it used
scripts/verify.sh 500 <prefix>    # prints OK or the check that failed
```

`seed.sh [count] [prefix]` posts an `OrderPlaced` for each order, then `PaymentCompleted` for even orders and `OrderCancelled` for odd ones. Each block of 50 events goes under its own API key so the seed stays under the rate limit. `verify.sh` checks once and does not wait for the Worker, so let consumer lag reach 0 first:

```
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group order-ledger
```

Check health and metrics:

```
curl http://localhost:8080/health
```

The API returns 200 with `{"kafka":"ok","redis":"ok"}` when it can reach Kafka, or 503 when it cannot. Redis reports `degraded` when it is down but never changes the status code. The API has no `/metrics` endpoint.

The Worker listens on port 8081 inside the compose network; the port is not published to the host. Query it through the container:

```
docker compose exec -T worker bash -c 'exec 3<>/dev/tcp/localhost/8081 && printf "GET /health HTTP/1.0\r\n\r\n" >&3 && cat <&3'
docker compose exec -T worker bash -c 'exec 3<>/dev/tcp/localhost/8081 && printf "GET /metrics HTTP/1.0\r\n\r\n" >&3 && cat <&3'
```

`/health` reports `kafka` and `postgres` as `ok` or `unavailable` and returns 503 if either is unavailable. `/metrics` returns the in-memory counters `processed`, `duplicates`, `retries`, `dead_lettered` and `dead_letter_write_retries`, which reset when the Worker restarts. The container image has no curl, which is why the commands open a raw TCP connection.

Run the tests (the integration tests start Kafka, PostgreSQL and Redis with Testcontainers, so Docker must be running):

```
dotnet test tests/EventPulse.UnitTests
dotnet test tests/EventPulse.IntegrationTests
```

## Guarantees and failure handling

The reasoning behind each of these is in [docs/decisions.md](docs/decisions.md).

- **At-least-once delivery, idempotent writes.** The consumer commits the PostgreSQL transaction first and the Kafka offset second, so a crash between the two causes a redelivery, never a loss. Each event's `eventId` is inserted into `processed_events` in the same transaction as the ledger change, so a redelivered event fails the primary key, is logged as a duplicate and is skipped.
- **Retry in place.** A transient failure, such as PostgreSQL being unreachable, retries the same message without advancing the offset: 6 attempts with waits of 0.5, 1, 2, 4 and 8 s, each attempt bounded by 2 s connection and command timeouts. This keeps events for one order in order, at the cost of blocking the partition for about 28 s during a full outage.
- **Dead letters.** A message that cannot be read is dead-lettered as `Malformed`, and a business conflict (for example a payment for an unknown order) as `Conflict`, both with no retries. A message that exhausts its retries is dead-lettered as `RetriesExhausted`. Each dead letter is written to the `dead_letters` table and published to `orders.events.dlq` with failure headers, then its offset is committed. If the `dead_letters` write itself fails, it is retried every 8 s up to a 4-minute deadline, after which the Worker stops without committing so the message is redelivered on restart. The `order-notifications` group has no dead-letter path; it keeps retrying under the same deadline.
- **Fail-open rate limiting.** `POST /events` is limited to 100 requests per 10-second fixed window per `X-Api-Key`, counted by one Lua script in Redis; over the limit returns 429 with `Retry-After`, and a missing key returns 400. If Redis is disconnected, slower than 200 ms or throws, the API logs `ratelimit bypassed`, increments a counter and accepts the request.
- **Graceful shutdown.** On stop, each consumer finishes the message it is on, commits its offset and calls `Close()` so the group rebalances immediately instead of waiting for the session timeout. A message that is waiting between retries when shutdown starts is left uncommitted and is redelivered. The host shutdown timeout is 10 s and compose gives the Worker 15 s before SIGKILL.

## Failure tests

Each script is in `tests/chaos/`, seeds its own orders under a fresh prefix, injects one failure, waits for both consumer groups to reach lag 0 and checks the result. Results are in [docs/failure-tests.md](docs/failure-tests.md).

- `kill-worker.sh [cycles]`: kills the Worker with SIGKILL 2 to 13 s into a seed, restarts it 5 s later and verifies the ledger, once per cycle.
- `duplicate-events.sh`: seeds, then resends the same 1,000 events with the same eventIds and checks the ledger changed once.
- `pause-postgres.sh seconds`: pauses PostgreSQL mid-seed; for 10 s or less it checks that every event is applied with no dead letters, for longer pauses it only reports.
- `stop-postgres-long.sh [seconds]`: stops PostgreSQL mid-seed (default 60 s), checks that only the expected dead letters are written and that events sent after the restart are processed normally.
- `stop-redis.sh [seconds]`: stops Redis mid-seed (default 30 s), checks the API fails open, then sends bursts under one key until limiting resumes with a 429.
- `poison.sh`: publishes 5 malformed messages directly to `orders.events` and checks each is dead-lettered once as `Malformed` without blocking its partition.

`tests/chaos/lib.sh` holds the shared helpers and is sourced by the scripts, not run directly.

## Measurements

Measured 2026-11-04 with `scripts/measure.sh`: 10,000 events (5,000 orders), three runs in each mode, medians reported.

| Measure | Mode | Median |
| --- | --- | --- |
| Worker throughput | Worker stopped during the seed, then started | 243.6 events/s |
| End-to-end latency p50 | `--live` | 35 ms |
| End-to-end latency p95 | `--live` | 68 ms |
| End-to-end latency p99 | `--live` | 93 ms |
| End-to-end latency max | `--live` | 343 ms |
| System throughput | `--live` | 35.5 events/s |

Worker throughput includes Worker startup (about 1.8 s) and one poll interval (about 0.9 s) of imprecision. Latency is measured from the seed's send time to `processed_events.processed_at`, which is taken before the commit, so real values are a few ms higher. System throughput in live mode is limited by the seed script, which starts one curl process per request, not by the Worker. Full output is in [docs/measurements/](docs/measurements/).

## Known limits

- If the API crashes after a successful publish but before responding, a client retry with a new `eventId` creates a duplicate event the system cannot detect.
- The API has no `/metrics` endpoint; the `ratelimit_bypassed` counter exists in code but is not exposed.
- The Worker's port 8081 is not published to the host; its `/health` and `/metrics` are reached with `docker compose exec`.
- Measurements come from a single local machine with all services in Docker; no hardware details were recorded.
- The chaos scripts `restart-kafka.sh`, `rebalance.sh` and `sigterm.sh` were not written, so Kafka broker restart and multi-instance rebalance are untested; graceful shutdown is covered only by the `ShutdownTests` integration test.
- Compose runs one Worker instance, and running several per consumer group has not been tested.
- Kafka is a single broker with replication factor 1 on every topic, so losing its data loses the event log and committed offsets.
- The API does not validate payload fields; an event with a bad payload gets 202 and is then dead-lettered as `Malformed` / `InvalidPayload`.
- Logs use the default console formatter, not JSON.
- If the dead-letter publish fails past the 4-minute deadline, a message with a readable `eventId` is skipped as a duplicate on restart and its dead letter is never published.
- The chaos scripts count only "Dead-letter write" log lines, so `poison.sh` does not detect dead-letter publish retries.
- An uncaught `KafkaException` from `Consume` or `Commit` stops the Worker; compose restarts it and the uncommitted message is redelivered.
- Offsets are committed synchronously, one message at a time; the effect on throughput has not been measured.
- `PaymentCompleted` amount and `paymentId` are not checked, and the cancel reason is not stored.
- `processed_events` is keyed on `event_id` alone, so it deduplicates for one consumer group only.
- Migrations run at Worker startup without a lock, so two Workers starting against a fresh database can race.

See [docs/design.md#known-limits](docs/design.md#known-limits) for details.

## Repo layout

```
src/
  EventPulse.Api/          HTTP API: validation, Redis rate limit, Kafka producer, /health
  EventPulse.Worker/       Kafka consumers, EF Core ledger and migrations, retry and dead-lettering, /health and /metrics
  EventPulse.Contracts/    Event envelope, payload records, topic names, JSON options
tests/
  EventPulse.UnitTests/         Unit tests for event serialization
  EventPulse.IntegrationTests/  Testcontainers tests against real Kafka, PostgreSQL and Redis
  chaos/                        Failure-injection scripts
scripts/                   seed.sh, verify.sh and measure.sh
docs/                      Design, decisions, failure-test results and measurement logs
.github/workflows/         CI: format check, build, unit and integration tests
docker-compose.yml         Kafka (KRaft), kafka-init, PostgreSQL, Redis, API and Worker
```
