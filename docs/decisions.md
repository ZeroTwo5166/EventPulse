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

## Notes for the README

- Copy .env.example to .env before docker compose up.
- Running the Worker with dotnet run needs PGPASSWORD set in the shell; the local connection string has no password.
- EF's CommandError and SaveChangesFailed events are lowered to Information, so the consumer's own failure handling must log at Error; EF will not.

