#!/usr/bin/env bash
# Sourced by the chaos scripts. Leaves the shell in the repository root.

cd "${BASH_SOURCE[0]%/*}/../.."

orders=500
lag_timeout=${CHAOS_LAG_TIMEOUT:-300}
ledger_category=EventPulse.Worker.Consumers.OrderLedgerConsumer
notifications_category=EventPulse.Worker.Consumers.OrderNotificationsConsumer

seed_out=$(mktemp)
trap 'rm -f "$seed_out"' EXIT

utc() {
  TZ=UTC printf '%(%Y-%m-%dT%H:%M:%SZ)T' -1
}

fresh() {
  printf 'chaos-%s-%(%s)T' "$1" -1
}

# Must stay identical to seed.sh.
fnv() {
  local s=$1 h=-3750763034362895579 i c
  for ((i = 0; i < ${#s}; i++)); do
    printf -v c '%d' "'${s:i:1}"
    ((h ^= c, h *= 1099511628211))
  done
  printf '%016x' "$h"
}

container() {
  local id
  id=$(docker compose ps -aq "$1")
  printf '%s\n' "${id%$'\r'}"
}

sql() {
  local out
  out=$(docker compose exec -T postgres psql -U eventpulse -d eventpulse -X -q -At -v ON_ERROR_STOP=1 "$@")
  printf '%s\n' "${out//$'\r'/}"
}

start_seed() {
  scripts/seed.sh "$orders" "$1" >"$seed_out" 2>&1 &
  seed_pid=$!
}

seed_running() {
  kill -0 "$seed_pid" 2>/dev/null
}

finish_seed() {
  local rc=0
  wait "$seed_pid" || rc=$?
  printf '%s\n' "$(<"$seed_out")"
  return "$rc"
}

# Summed lag of the group on orders.events, or "-" while a partition that has messages has no committed offset.
lag() {
  local out topic end l total=0 seen=0
  if ! out=$(docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group "$1" 2>/dev/null); then
    echo -
    return
  fi
  while read -r _ topic _ _ end l _; do
    [[ $topic == orders.events ]] || continue
    seen=1
    if [[ $l =~ ^[0-9]+$ ]]; then
      total=$((total + l))
    elif [[ $end != 0 ]]; then
      echo -
      return
    fi
  done <<<"${out//$'\r'/}"
  if ((seen)); then echo "$total"; else echo -; fi
}

wait_lag() {
  local group=$1 start=$SECONDS last="" now
  while :; do
    now=$(lag "$group")
    if [[ $now != "$last" ]]; then
      echo "lag $group=$now at $((SECONDS - start))s"
      last=$now
    fi
    [[ $now == 0 ]] && return 0
    if ((SECONDS - start >= lag_timeout)); then
      echo "lag $group did not reach 0 within ${lag_timeout}s"
      return 1
    fi
    sleep 2
  done
}

# verify.sh also counts notifications, so both groups must have caught up.
wait_lags() {
  wait_lag order-ledger && wait_lag order-notifications
}

verify() {
  local out rc=0
  out=$(scripts/verify.sh "$orders" "$1" 2>&1) || rc=$?
  printf 'verify.sh %s %s:\n%s\n' "$orders" "$1" "$out"
  return "$rc"
}

# Lines from the service's log since the time that contain the text, optionally only those logged under
# the category. The console logger writes "level: Category[id]" on one line and the message on the next.
logs() {
  local service=$1 since=$2 text=$3 category=${4:-} line current=""
  local header='^(trce|dbug|info|warn|fail|crit): ([^[]+)\['
  while IFS= read -r line; do
    line=${line%$'\r'}
    if [[ $line =~ $header ]]; then
      current=${BASH_REMATCH[2]}
      continue
    fi
    [[ $line == *"$text"* ]] || continue
    [[ -z $category || $current == "$category" ]] || continue
    printf '%s\n' "${line#"${line%%[![:space:]]*}"}"
  done < <(docker compose logs --no-log-prefix --no-color --since "$since" "$service" 2>&1)
}

# Prints the count, then up to five of the lines.
report() {
  local label=$1 i
  shift
  echo "$label=$#"
  for ((i = 1; i <= $# && i <= 5; i++)); do
    echo "  ${!i}"
  done
}
