#!/usr/bin/env bash
# Usage: stop-postgres-long.sh [seconds]
# Failure test 5. Seeds a fresh prefix, docker stops postgres 5 s into the seed for the given number of seconds
# (default 60), starts it again and waits for pg_isready. Then seeds a second small prefix, waits for lag 0 in
# both consumer groups and runs verify.sh on both prefixes.
# Pass, all of:
#   1. Postgres is ready within 60 s of docker start.
#   2. Both groups reach lag 0.
#   3. dead_letters has at least one RetriesExhausted row for the first prefix.
#   4. Every dead_letters row for the first prefix is RetriesExhausted, or Conflict/OrderNotFound for an order whose
#      OrderPlaced was dead-lettered as RetriesExhausted.
#   5. Every order verify.sh would count as wrong has a dead_letters row, and verify.sh's processed_events and
#      notifications checks do not fail.
#   6. No "Deadline passed" line.
#   7. The second prefix, seeded after the restart, verifies OK with no dead letters.
set -euo pipefail

here=${BASH_SOURCE[0]%/*}
[[ $here != "${BASH_SOURCE[0]}" ]] || here=.
. "$here/lib.sh"

outage=${1:-60}
if [[ ! $outage =~ ^[1-9][0-9]*$ ]]; then
  echo "seconds must be a positive integer, got '$outage'" >&2
  exit 2
fi

delay=5
small=20
ready_timeout=60
postgres=$(container postgres)
stopped=0

# Replaces lib.sh's trap, so it also removes the seed output. An interrupted run must not leave Postgres down.
cleanup() {
  if ((stopped)); then
    docker start "$postgres" >/dev/null || true
  fi
  rm -f "$seed_out"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

prefix=$(fresh stop)
after=$prefix-after
since=$(utc)
echo "outage=${outage}s prefix=$prefix since=$since"
if ((outage < 20)); then
  echo "note: ${outage}s is shorter than the retry schedule, so condition 3 is not expected to hold"
fi

start_seed "$prefix"
sleep "$delay"
running=no
seed_running && running=yes
stopping_at=$SECONDS
stopped=1
docker stop "$postgres" >/dev/null
stopped_at=$SECONDS
echo "postgres stopped ${delay}s into the seed in $((stopped_at - stopping_at))s, seed still running: $running"
sleep "$outage"
docker start "$postgres" >/dev/null
stopped=0
started_at=$SECONDS
echo "postgres started after $((started_at - stopped_at))s down"

ready=never
while ((SECONDS - started_at < ready_timeout)); do
  if docker compose exec -T postgres pg_isready -q -U eventpulse -d eventpulse >/dev/null 2>&1; then
    ready=$((SECONDS - started_at))s
    break
  fi
  sleep 1
done
echo "postgres ready $ready after docker start"
if [[ $ready == never ]]; then
  echo "postgres did not accept connections within ${ready_timeout}s"
  echo FAIL
  exit 1
fi

finish_seed || true
echo "second seed after restart prefix=$after"
scripts/seed.sh "$small" "$after" || true

caught_up=never
caught_up_stop=never
if wait_lags; then
  caught_up=$((SECONDS - started_at))s
  caught_up_stop=$((SECONDS - stopped_at))s
  echo "lag 0 on both groups $caught_up after start, $caught_up_stop after stop"
fi
# The console logger writes from a background queue, so the last lines can trail the offset commit.
sleep 2

verified=0
if out=$(verify "$prefix"); then
  verified=1
fi
printf '%s\n' "$out"
other_failed=0
while IFS= read -r line; do
  if [[ $line == "FAIL processed_events for order-ledger:"* || $line == "FAIL notifications:"* ]]; then
    other_failed=$((other_failed + 1))
  fi
done <<<"$out"

after_verified=0
if out=$(scripts/verify.sh "$small" "$after" 2>&1); then
  after_verified=1
fi
printf 'verify.sh %s %s:\n%s\n' "$small" "$after" "$out"

retries "$since"
dead_letters "$prefix"
first_dead=$dead

row=$(sql -c "
select
  count(*) filter (where reason = 'RetriesExhausted'),
  count(*) filter (where reason <> 'RetriesExhausted' and not (reason = 'Conflict' and detail = 'OrderNotFound')),
  count(*) filter (where reason = 'Conflict' and detail = 'OrderNotFound' and not exists (
    select 1 from dead_letters r
    where r.order_id = d.order_id and r.reason = 'RetriesExhausted' and r.event_type = 'OrderPlaced'))
from dead_letters d
where $(of_prefix d.order_id "$prefix")")
IFS='|' read -r exhausted_rows unexpected_rows orphan_rows <<<"$row"
echo "RetriesExhausted rows=$exhausted_rows rows not RetriesExhausted or Conflict/OrderNotFound=$unexpected_rows OrderNotFound rows without a RetriesExhausted OrderPlaced=$orphan_rows"

row=$(sql -c "
select count(*), coalesce(string_agg('${prefix}_' || s.i, ' ' order by s.i), '')
from generate_series(1, $orders) s(i)
left join orders o on o.order_id = '${prefix}_' || s.i
where (o.order_id is null or o.version <> 2
    or o.status <> case when s.i % 2 = 0 then 'Paid' else 'Cancelled' end)
  and not exists (select 1 from dead_letters d where d.order_id = '${prefix}_' || s.i)")
IFS='|' read -r unaccounted unaccounted_ids <<<"$row"
echo "wrong orders without a dead letter=$unaccounted"
if ((unaccounted > 0)); then
  echo "  $unaccounted_ids"
fi
echo "verify processed_events or notifications failures=$other_failed"

dead_letters "$after"
after_dead=$dead
echo

c1=0 c2=0 c3=0 c4=0 c5=0 c6=0 c7=0
[[ $ready != never ]] && c1=1
[[ $caught_up != never ]] && c2=1
((exhausted_rows >= 1)) && c3=1
((unexpected_rows == 0 && orphan_rows == 0)) && c4=1
((unaccounted == 0 && other_failed == 0)) && c5=1
((deadline == 0)) && c6=1
((after_verified && after_dead == 0)) && c7=1

echo "outage=$outage verify_ok=$verified dead_letters=$first_dead retries_order_ledger=$retries_ledger retries_order_notifications=$retries_notifications retries_total=$((retries_ledger + retries_notifications)) dead_letter_write_retries=$dead_letter_writes retries_exhausted=$exhausted deadline_passed=$deadline ready_after_start=$ready lag0_after_start=$caught_up lag0_after_stop=$caught_up_stop"
echo "condition 1 postgres ready within ${ready_timeout}s: $c1 ($ready)"
echo "condition 2 lag 0 on both groups: $c2 ($caught_up after start)"
echo "condition 3 at least one RetriesExhausted dead letter: $c3 ($exhausted_rows)"
echo "condition 4 only RetriesExhausted and its OrderNotFound knock-ons: $c4 (unexpected=$unexpected_rows orphan=$orphan_rows)"
echo "condition 5 every verify failure has a dead letter: $c5 (unaccounted orders=$unaccounted other checks failed=$other_failed)"
echo "condition 6 no deadline passed: $c6 ($deadline)"
echo "condition 7 second prefix after restart: $c7 (verify_ok=$after_verified dead_letters=$after_dead)"
if ((c1 && c2 && c3 && c4 && c5 && c6 && c7)); then
  echo PASS
  exit 0
fi
echo FAIL
exit 1
