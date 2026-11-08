# Decisions

## 2026-10-22 Two deployable projects

The API and the Worker are separate projects that share only the Kafka log.
The API validates and publishes; it never opens a Postgres connection. The
Worker consumes and writes the ledger; it never serves HTTP. The alternative
was one process with a background consumer, which is simpler to run but makes
the crash tests meaningless: killing the consumer would also kill ingestion.
2026-11-02: the Worker now also listens on 8081 for /health and /metrics, used
only by compose health checks; it still serves no business traffic.

## 2026-10-22 Kafka in KRaft mode

Single broker, no ZooKeeper. One fewer container to configure and keep healthy,
and it is the mode every new Kafka deployment uses. One consequence: the
container's only client listener advertises the host-mapped port, so running
kafka-topics inside the container does not reach the broker under
Testcontainers. The integration fixture creates topics with AdminClient instead.

## 2026-10-22 No schema registry

Events are JSON with a schemaVersion field in the envelope and eventType in a
Kafka header. A registry with Avro would add a container and a code-generation
step to solve a problem three event types do not have. If a breaking change ever
comes, schemaVersion is where the consumer branches.

## 2026-10-22 Formatting rules

Line endings are LF, enforced by .gitattributes, because dotnet add package and
some Windows tooling write CRLF and the CI format check rejects it. Private
static readonly fields are PascalCase, with an .editorconfig rule that overrides
the general _camelCase rule for private fields; the three existing fields were
already written that way.

## 2026-10-22 Offsets are committed after the database transaction

The consumer commits the Postgres transaction first and the Kafka offset
second. If the worker dies between the two, the message is redelivered and the
processed_events primary key rejects it as a duplicate. The reverse order
would lose the event on a crash: offset committed, ledger unchanged, Kafka
never resends it. At-least-once from Kafka, exactly-once on the ledger.

## 2026-10-22 One message at a time per consumer

Each consumer instance processes a single message to completion before
consuming the next. Parallelism comes from partitions and from running more
worker instances, not from concurrent handlers inside one consumer. This keeps
the offset-commit reasoning simple: the only in-flight message is the one whose
offset has not been committed yet.

## 2026-10-22 Duplicate detection by unique violation

processed_events is inserted and saved before the ledger change, inside the
same transaction. A redelivery fails the primary key, the transaction rolls
back and the offset is committed. The alternative, ON CONFLICT DO NOTHING, is
quieter but hides the fact that a redelivery happened; the explicit violation is
logged and counted. EF's own error logging for the two failure events is lowered
to Information so a duplicate does not look like an outage. EF then no longer
reports failed commands or saves at Error, so the consumer logs exhausted
retries, dead letters and passed deadlines at Error itself.

## 2026-11-02 Retry in place, not a retry topic

A transient failure retries the same message in the consumer loop without
advancing the offset: six attempts with waits of 0.5, 1, 2, 4 and 8 seconds,
then dead-letter and commit. A retry topic was rejected because it breaks
per-order ordering: a retried OrderPlaced can be reprocessed after the
PaymentCompleted that followed it on the main topic. The cost is head-of-line
blocking, about 28 seconds per message during a full Postgres outage. For an
order ledger, ordering is worth more than partition throughput during an outage.
A retry topic is the right choice when the opposite holds.

## 2026-11-02 Rate limit fails open

If Redis is disconnected, the Lua script takes longer than 200 ms, or the call
fails with a RedisException, the API logs a warning, increments
ratelimit_bypassed and accepts the request without counting it. The rate limit
protects Kafka from a noisy client; it is not a security boundary, and refusing
all traffic because a cache is down is the worse failure. Fail closed would be
right if the limit enforced a paid quota or guarded an expensive backend, where
over-admitting costs more than an outage. The multiplexer is created with
AbortOnConnectFail false so the API starts while Redis is down and reconnects
in the background. Verified 2026-11-01 with tests/chaos/stop-redis.sh.

## 2026-11-02 Fixed window, not sliding

The limit is 100 requests per 10-second window per API key, keyed by
ratelimit:{key}:{floor(now / 10)} and counted with one Lua script that does INCR
and sets EXPIRE on the first hit. A fixed window lets a client send up to 200
requests in the two seconds around a boundary. A sliding log or sliding window
would remove that burst but needs a sorted set per key or two counters and a
weighted sum, and the burst does not matter for a limit whose purpose is to keep
one client from flooding Kafka. The script is atomic so a dropped connection
between INCR and EXPIRE cannot leave a key that never expires.

## 2026-11-06 Dead-letter publish retried on its own

A dead letter is written in two steps: the dead_letters row and its
processed_events row are committed together, then the message is published to
orders.events.dlq and dlq_published_at is set. These used to run as one retried
step, so a failed publish repeated the insert, the insert hit the
processed_events key as a duplicate, and the offset was committed without the
message ever reaching the DLQ. A message with no readable eventId got a second
dead_letters row instead. Now only the publish and the timestamp update are
retried, with the same backoff capped at 8 s and the same 4-minute deadline
from consume, and a publish that succeeded is not repeated when only the update
failed. Each retry counts in dead_letter_write_retries. The Worker's producer
has a 10-second message timeout (Kafka:ProduceTimeout); without it librdkafka
waits up to its 5-minute default, past both the deadline and
max.poll.interval.ms. Past the deadline the Worker stops without committing.
On restart a message with a readable eventId is skipped as a duplicate, so its
dead letter stays in the table with dlq_published_at null and is never
published.
