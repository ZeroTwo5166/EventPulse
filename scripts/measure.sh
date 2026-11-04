#!/usr/bin/env bash
# Usage: measure.sh [--live] [orders] [label]
# Throughput and end-to-end latency for orders * 2 events, default 5000 orders (10,000 events). Seeds a fresh prefix
# with SEND_LOG set, polls the worker's /metrics processed counter until it has grown by the number of events, then
# loads the send log into Postgres and joins it to processed_events.
# Without --live, for worker throughput: the worker is stopped during the seed and started after it.
#   worker throughput: events / (finished - worker started), at the resolution of one poll.
#   Latency then includes the time each event waited in Kafka for the worker to start.
# With --live, for end-to-end latency: the worker runs throughout and worker throughput is not reported.
# latency: processed_at - send time per event.
# system throughput: events / (max(processed_at) - min(send time)).
set -euo pipefail

here=${BASH_SOURCE[0]%/*}
[[ $here != "${BASH_SOURCE[0]}" ]] || here=.
. "$here/../tests/chaos/lib.sh"

live=0
args=()
for a in "$@"; do
  if [[ $a == --live ]]; then live=1; else args+=("$a"); fi
done
orders=${args[0]:-5000}
label=${args[1]:-run}
if [[ ! $orders =~ ^[1-9][0-9]*$ ]]; then
  echo "orders must be a positive integer, got '$orders'" >&2
  exit 2
fi
if [[ ! $label =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "label may contain only letters, digits, '_' and '-', got '$label'" >&2
  exit 2
fi

events=$((orders * 2))
timeout=600
prefix=measure-$label-$(printf '%(%s)T' -1)
mode=stopped
if ((live)); then mode=live; fi
send_log=$(mktemp)
worker=$(container worker)
stopped=0

# Replaces lib.sh's trap, so it also removes the seed output. An interrupted run must not leave the worker stopped.
cleanup() {
  if ((stopped)); then
    docker start "$worker" >/dev/null || true
  fi
  rm -f "$seed_out" "$send_log"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Epoch milliseconds into the named variable, without a subshell.
stamp() {
  local t=$EPOCHREALTIME
  printf -v "$1" '%s%s' "${t%[.,]*}" "${t: -6:3}"
}

# Sets count to the worker's processed counter. Fails while the worker is not serving yet.
processed() {
  local body
  body=$(docker compose exec -T worker bash -c \
    'exec 3<>/dev/tcp/localhost/8081 && printf "GET /metrics HTTP/1.0\r\n\r\n" >&3 && cat <&3' 2>/dev/null) || return 1
  [[ $body =~ \"processed\":([0-9]+) ]] || return 1
  count=${BASH_REMATCH[1]}
}

# One decimal of $1 events over $2 ms.
rate() {
  local tenths=$(($1 * 10000 / $2))
  printf '%d.%d' $((tenths / 10)) $((tenths % 10))
}

echo "measure label=$label prefix=$prefix orders=$orders events=$events mode=$mode"

# processed_at comes from the worker's clock, which is the same Docker VM clock Postgres reads.
stamp before
pg=$(sql -c "select (extract(epoch from clock_timestamp()) * 1000)::bigint")
stamp after
offset=$((pg - (before + after) / 2))
rtt=$((after - before))
echo "clock offset postgres-host=${offset}ms (round trip ${rtt}ms)"

# A backlog left from an earlier run would count toward processed, and with --live it would also delay this run's events.
backlog=$(lag order-ledger)
if [[ $backlog != 0 ]]; then
  echo "order-ledger lag is $backlog, not 0; wait for the worker to catch up first" >&2
  exit 1
fi

# A stopped worker restarts its counter at 0; a running one keeps counting from where it is.
base=0
if ((live)); then
  if ! processed; then
    echo "could not read processed from the worker's /metrics" >&2
    exit 1
  fi
  base=$count
  echo "worker running, processed=$base before the seed"
else
  # Same grace as stop_grace_period in docker-compose.yml, so the worker shuts down cleanly and leaves the group.
  docker stop -t 15 "$worker" >/dev/null
  stopped=1
  echo "worker stopped"
fi

stamp seed_start
SEND_LOG=$send_log scripts/seed.sh "$orders" "$prefix"
stamp started
seed_ms=$((started - seed_start))
echo "seed took ${seed_ms}ms"

if ((!live)); then
  docker start "$worker" >/dev/null
  stopped=0
  echo "worker started"
fi

count=$base
last=-1
while :; do
  processed || true
  stamp now
  if ((count != last)); then
    echo "processed=$((count - base)) at $((now - started))ms"
    last=$count
  fi
  ((count - base >= events)) && break
  if ((now - started >= timeout * 1000)); then
    echo "processed=$((count - base)) did not reach $events within ${timeout}s" >&2
    exit 1
  fi
  sleep 0.5
done
finished=$now
worker_ms=$((finished - started))

row=$({
  echo "create temp table sends (event_id uuid primary key, sent_ms bigint);"
  echo "copy sends from stdin with (delimiter ' ');"
  cat "$send_log"
  echo '\.'
  cat <<'SQL'
with m as (
  select s.sent_ms, p.processed_at, (extract(epoch from p.processed_at) * 1000 - s.sent_ms)::float8 as ms
  from sends s
  join processed_events p on p.event_id = s.event_id and p.consumer_group = 'order-ledger'
)
select
  count(*),
  round(percentile_cont(0.5) within group (order by ms)::numeric),
  round(percentile_cont(0.95) within group (order by ms)::numeric),
  round(percentile_cont(0.99) within group (order by ms)::numeric),
  round(max(ms)::numeric),
  round(count(*) * 1000 / (extract(epoch from max(processed_at)) * 1000 - min(sent_ms)), 1)
from m;
SQL
} | sql)
IFS='|' read -r matched p50 p95 p99 max system <<<"${row##*$'\n'}"

echo
echo "label=$label prefix=$prefix orders=$orders events=$events mode=$mode"
echo "clock offset postgres-host: ${offset} ms (round trip ${rtt} ms)"
echo "seed duration: ${seed_ms} ms"
if ((!live)); then
  echo "worker throughput: $(rate "$events" "$worker_ms") events/s ($events events in ${worker_ms} ms)"
fi
echo "events matched: $matched of $events"
echo "end-to-end latency ms: p50=$p50 p95=$p95 p99=$p99 max=$max"
echo "system throughput: $system events/s"
((matched == events))
