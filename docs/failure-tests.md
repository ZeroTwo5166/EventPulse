# Failure tests

Date run: 2026-11-01

Each test is a bash script in `tests/chaos/`. A script seeds its own orders, injects one failure, waits for both consumer groups to reach lag 0 and then checks the result. It prints `PASS` and exits 0, or prints `FAIL` and exits 1. A bad argument exits 2.

## Running the scripts

1. Copy `.env.example` to `.env` and set `POSTGRES_PASSWORD`.
2. Start the stack with `docker compose up -d --build` and wait until the api and worker containers are running.
3. Run a script from bash (Git Bash on Windows) with `docker compose` and `curl` on the path. The scripts work from any directory; they change to the repository root themselves.

Every run seeds 500 orders (1,000 events) under a fresh prefix, `chaos-<name>-<unix time>`, so runs do not interfere with each other and the database does not need resetting between them. `seed.sh` sends each block of 50 events under its own API key, so the seed itself never reaches the rate limit.

`verify.sh` checks that all 500 orders exist, even orders are `Paid` and odd orders `Cancelled`, every order is at version 2, `order-ledger` has 1,000 `processed_events` rows for the prefix and there are 1,000 notifications.

Two environment variables change the defaults:

- `CHAOS_LAG_TIMEOUT`: seconds to wait for lag 0 in each group, default 300.
- `EVENTPULSE_API`: the API address, default `http://localhost:8080`.

`pause-postgres.sh`, `stop-postgres-long.sh` and `stop-redis.sh` unpause or start their container again if the script is interrupted. `kill-worker.sh` does not, so after a Ctrl-C run `docker compose start worker`.

## kill-worker (test 1)

Command:

```
tests/chaos/kill-worker.sh 3
```

Injects: three cycles. Each cycle starts a fresh seed, kills the worker with `docker kill` (SIGKILL) at a random point 2 to 13 s into it and starts the worker again 5 s later. Each cycle is its own seed rather than a second kill in the same seed, because the restarted worker gets no partitions until the killed member's session times out, and that takes longer than a seed.

Expected: the ledger is correct after every kill. A kill between the Postgres commit and the offset commit leads to a redelivery that is skipped as a duplicate. A kill inside a transaction is rolled back by Postgres and the event is applied once when it is redelivered. Tests goal 1 and the guarantees rows "Worker killed after DB commit, before offset commit" and "Worker killed mid-transaction".

Pass/fail: PASS if `verify.sh` prints OK in all 3 cycles. Each cycle also prints the `Duplicate` lines from both consumer groups and the Postgres lines "unexpected EOF on client connection with an open transaction". These show which window the kill landed in but are not required: the gap between the two commits is a few milliseconds, so a cycle can pass with neither.

What happened: PASS. `verify.sh` printed OK in all 3 cycles, with the worker killed 12 s, 11 s and 7 s into the seed. Both groups logged 0 `Duplicate` lines, Postgres logged 0 open-transaction EOF lines, and `order-ledger` lag reached 0 36 s, 36 s and 29 s after each seed finished.

## duplicate-events (test 2)

Command:

```
tests/chaos/duplicate-events.sh
```

Injects: seeds 500 orders, waits for lag 0, then runs `seed.sh` again with the same prefix. The second run resends the same 1,000 events under the same eventIds.

Expected: the API accepts the replay with 202 and the ledger changes once. The second delivery of each event hits the `processed_events` primary key and is skipped. Tests goal 2 and the guarantees row "Same event POSTed twice".

Pass/fail: PASS if all of these hold:

- `verify.sh` prints OK.
- `processed_events` has 1,000 rows for the prefix, not 2,000.
- `order-ledger` logs 1,000 "Duplicate ... skipped" lines during the replay.
- `order-notifications` logs 1,000 "Duplicate ... skipped" lines during the replay.

`seed.sh` prints the 202 count for the replay, but the script does not check it.

What happened: PASS. The replay got 202 for all 1,000 requests, `processed_events` stayed at 1,000 rows for the prefix, and each group logged 1,000 "Duplicate ... skipped" lines during the replay. `verify.sh` printed OK, and both groups were at lag 0 within 2 s of the replay finishing.

## pause-postgres, 10 s (test 4)

Command:

```
tests/chaos/pause-postgres.sh 10
```

Injects: starts a seed, runs `docker pause` on postgres 5 s into it, and unpauses it 10 s later. A paused Postgres does not answer, so every attempt fails on the 2 s connection or command timeout.

Expected: both consumers retry the current message in place with backoff and succeed once Postgres is back. Nothing is dead-lettered. Tests the guarantees row "Postgres down for 10 s" (6 attempts, waits sum to 15.5 s) and step 5 of the processing loop.

Pass/fail: PASS if `verify.sh` prints OK and `dead_letters` has no rows for the prefix. The script also prints the retry-in-place lines for each group, any dead-letter write retries, and how long after the unpause both groups reached lag 0.

What happened: PASS. The pause lasted 11 s, and both groups retried the same `OrderPlaced` 3 times in place, with 0 dead-letter write retries and 0 dead letters. `verify.sh` printed OK, and both groups were at lag 0 18 s after the unpause.

## pause-postgres, 60 s (test 4)

Command:

```
tests/chaos/pause-postgres.sh 60
```

Injects: the same as the 10 s run, with a 60 s pause.

Expected: the pause is longer than the retry schedule, which gives up after about 28 s. The ledger consumer dead-letters the message it is on as `RetriesExhausted`. The `dead_letters` write also fails while Postgres is paused, so it is retried every 8 s until Postgres is back, well inside the 4-minute deadline. The notifications consumer has no dead-letter path and keeps retrying at the 8 s cap. Once Postgres is back, the partition continues. Tests the guarantees row "Postgres down for 60 s", steps 5 and 6 of the processing loop and "When the dead-letter write itself fails".

Pass/fail: there is no pass condition. For any pause over 10 s the script prints `REPORT` and exits 0. `verify.sh` can fail here, because a dead-lettered event is missing from the ledger. The report shows the `verify.sh` output, the `dead_letters` rows for the prefix, the retry counts per group, the dead-letter write retries, any "Retries exhausted" and "Deadline passed" lines, and how long after the unpause both groups reached lag 0.

What happened: REPORT. The `OrderPlaced` for `chaos-pause-1793491698_192` exhausted its 6 attempts and was dead-lettered as `RetriesExhausted` after 3 dead-letter write retries, with detail `Npgsql.NpgsqlException: Exception while reading from stream ---> System.TimeoutException: Timeout during reading attempt`; its `PaymentCompleted` followed as `Conflict` / `OrderNotFound` with 1 attempt, which matches the knock-on row in the design.md guarantees table. `verify.sh` found 499 of 500 orders, 249 of 250 even orders `Paid` and 499 at version 2, explained by that dead letter. The pause lasted 61 s, `order-notifications` retried 9 times, no deadline passed, and both groups were at lag 0 9 s after the unpause.

## stop-postgres-long (test 5)

Command:

```
tests/chaos/stop-postgres-long.sh 60
```

Injects: starts a seed, runs `docker stop` on postgres 5 s into it, keeps it stopped for 60 s, then runs `docker start` and waits for `pg_isready`. It then seeds a second prefix, `<prefix>-after`, of 20 orders. Unlike a pause, a stop closes every connection and Postgres has to start up again.

Expected: the ledger consumer dead-letters the message it is on as `RetriesExhausted`, and the dead-letter write is retried until Postgres is back. The partition continues afterwards. If the exhausted message was an `OrderPlaced`, the order's later `PaymentCompleted` or `OrderCancelled` is dead-lettered as `Conflict` / `OrderNotFound` with no retries. Events sent after the restart are processed normally. Tests the guarantees rows "Postgres down for 60 s" and "Postgres down long enough that an `OrderPlaced` is dead-lettered as `RetriesExhausted`", and "When the dead-letter write itself fails".

Pass/fail: PASS if all seven conditions hold:

1. Postgres is ready within 60 s of `docker start`.
2. Both groups reach lag 0.
3. `dead_letters` has at least one `RetriesExhausted` row for the first prefix.
4. Every `dead_letters` row for the first prefix is `RetriesExhausted`, or `Conflict` / `OrderNotFound` for an order whose `OrderPlaced` was dead-lettered as `RetriesExhausted`.
5. Every order `verify.sh` would count as wrong has a `dead_letters` row, and the `processed_events` and notifications checks in `verify.sh` do not fail.
6. No "Deadline passed" line is logged.
7. The second prefix verifies OK and has no dead letters.

`verify.sh` on the first prefix is expected to fail. Its output is printed but it is not a condition.

What happened: PASS, all seven conditions; Postgres was down for 61 s and ready 0 s after `docker start`. The `OrderPlaced` for `chaos-stop-1793491801_188` was dead-lettered as `RetriesExhausted` after 6 attempts and 3 dead-letter write retries, with detail `System.Net.Sockets.SocketException: Name or service not known`, and its `PaymentCompleted` as `Conflict` / `OrderNotFound` with 1 attempt; there were no other dead letters and no deadline passed. `verify.sh` found 499 of 500 orders on the first prefix, every wrong order had a dead letter, the 20-order second prefix verified OK with 0 dead letters, and both groups were at lag 0 14 s after the start.

## stop-redis (test 6)

Command:

```
tests/chaos/stop-redis.sh 30
```

Injects: starts a seed, runs `docker stop` on redis 5 s into it, keeps it stopped for 30 s, then runs `docker start` and waits for PING. It then sends bursts of 250 `OrderPlaced` requests under one API key, each burst under its own prefix, until a burst gets a 429 or 6 bursts have run, 10 s apart. A burst takes about 5 s, so it spans at most two 10 s windows and one of them must go over the limit of 100.

Expected: while Redis is down, the API fails open. It accepts every request, logs a "ratelimit bypassed" warning and counts the bypass. After Redis is back, limiting resumes without an API restart. The worker is not affected. Tests goal 5, the guarantees row "Redis down" and "When Redis is down: fail open".

Pass/fail: PASS if all seven conditions hold:

1. Redis answers PING within 60 s of `docker start`.
2. The seed gets 202 for every request and nothing else.
3. The API logs at least one "ratelimit bypassed" line after the stop.
4. Both groups reach lag 0.
5. `verify.sh` prints OK.
6. `dead_letters` has no rows for the prefix.
7. A burst that starts within 60 s of PONG gets at least one 429.

What happened: PASS, all seven conditions. Redis was down for 31 s; the seed got 202 for all 1,000 requests, the API logged 909 "ratelimit bypassed, Redis disconnected" lines from the stop on, 107 of them after `docker start`, `verify.sh` printed OK with 0 dead letters, and both groups were at lag 0 26 s after the start. The first burst after PONG got 250 202s and 0 429s, and the first 429s came in the second burst, which started 17 s after PONG and got 100 202s and 150 429s, so the limiter kept failing open until the Redis client reconnected.

## poison (test 7)

Command:

```
tests/chaos/poison.sh
```

Injects: starts a seed and, 5 s into it, publishes 5 malformed messages straight to `orders.events` with `kafka-console-producer`, which bypasses the API's validation. There is one message for each `Malformed` detail the worker distinguishes:

1. `InvalidEnvelope`: the body is not JSON.
2. `MissingEventId`: no eventId.
3. `UnknownEventType`: eventType `OrderShipped`.
4. `MissingOrderId`: no orderId.
5. `InvalidPayload`: an `OrderPlaced` without a currency.

They are keyed `<prefix>-poison_1` to `_5`. When the body has no orderId, the worker uses the key as the dead letter's `order_id`.

Expected: each message is dead-lettered immediately as `Malformed` with no retries and published to the DLQ, and its offset is committed. The partition carries on and the seed completes. `order-notifications` skips the messages without writing anything. Tests goal 4, the guarantees row "Malformed JSON published" and step 2 of the processing loop.

Pass/fail: PASS if all six conditions hold:

1. Each poison message has exactly one `dead_letters` row: `Malformed`, its expected detail, attempts 1, DLQ published.
2. No retry-in-place or dead-letter write retry line mentions a poison message.
3. `order-notifications` logs a skip at each poison message's partition and offset, and writes no notification for them.
4. `order-ledger` applied a seed event after each poison message on the same partition.
5. Both groups reach lag 0.
6. `verify.sh` prints OK for the seed prefix and `dead_letters` has no rows for it.

For condition 4, the partition and offset of each poison message come from the worker log. The console producer and the API's producer hash keys differently, so a poison message cannot be put on a chosen order's partition.

What happened: PASS, all six conditions. Each of the 5 messages got one `dead_letters` row as `Malformed` with its expected detail (`InvalidEnvelope`, `MissingEventId`, `UnknownEventType`, `MissingOrderId`, `InvalidPayload`), 1 attempt and DLQ published, with 0 retry lines and 0 notifications for them; `order-notifications` skipped each one at its partition and offset, and `order-ledger` applied later seed events on all five partitions they landed on (0, 1, 2, 4, 5). `verify.sh` printed OK for the seed with 0 dead letters, and both groups were at lag 0 26 s after the poison was published.
