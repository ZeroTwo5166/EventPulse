# Decisions

## 2026-10-22 Two deployable projects

The API and the Worker are separate projects that share only the Kafka log.
The API validates and publishes; it never opens a Postgres connection. The
Worker consumes and writes the ledger; it never serves HTTP. The alternative
was one process with a background consumer, which is simpler to run but makes
the crash tests meaningless: killing the consumer would also kill ingestion.

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
to Information so a duplicate does not look like an outage.


## Notes for the README

- Copy .env.example to .env before docker compose up.
- Running the Worker with dotnet run needs PGPASSWORD set in the shell; the local connection string has no password.
- EF's CommandError and SaveChangesFailed events are lowered to Information, so the consumer's own failure handling must log at Error; EF will not.
- seed.sh sends 1,000 requests in about 20 s; once the rate limit exists (100 per 10 s per key), seed and chaos scripts need a separate key per batch or a configurable limit.
- verify.sh checks once and does not wait for the worker; chaos scripts must wait for consumer lag to reach 0 before calling it.
