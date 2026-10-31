#!/usr/bin/env bash
# Usage: poison.sh
# Failure test 7. Seeds a fresh prefix and, 5 s into the seed, publishes 5 malformed messages straight to
# orders.events with kafka-console-producer, one per Malformed detail the worker distinguishes. They are keyed
# {prefix}-poison_1..5, which the worker falls back to as order_id when the body has none.
# The console producer hashes keys with murmur2 and the API's librdkafka producer with CRC32, so a poison message
# cannot be keyed onto a chosen seed order's partition. Every partition carries seed events, so instead each poison
# message's partition and offset are read from the worker's log and compared with the seed events applied there.
# Pass, all of:
#   1. Each poison message has exactly one dead_letters row: Malformed, its expected detail, attempts 1, DLQ published.
#   2. No retry-in-place or dead-letter write retry line mentions a poison message.
#   3. order-notifications logs a skip at each poison message's partition and offset and writes no notification.
#   4. order-ledger applied a seed event after each poison message on the same partition.
#   5. Both groups reach lag 0.
#   6. verify.sh prints OK for the seed prefix and dead_letters has no rows for it.
set -euo pipefail

here=${BASH_SOURCE[0]%/*}
[[ $here != "${BASH_SOURCE[0]}" ]] || here=.
. "$here/lib.sh"

delay=5
prefix=$(fresh poison)
pp=$prefix-poison
since=$(utc)
echo "prefix=$prefix poison=$pp since=$since"

h=$(fnv "$pp")
now=$(utc)
id() {
  printf '%s-%s-8%s-8009-%012x' "${h:0:8}" "${h:8:4}" "${h:12:3}" "$1"
}
expected=(
  ""
  InvalidEnvelope
  MissingEventId
  UnknownEventType
  MissingOrderId
  InvalidPayload
)
bodies=(
  ""
  "not json ${pp}_1"
  "{\"eventType\":\"OrderPlaced\",\"orderId\":\"${pp}_2\"}"
  "{\"eventId\":\"$(id 3)\",\"eventType\":\"OrderShipped\",\"orderId\":\"${pp}_3\",\"occurredAt\":\"$now\",\"schemaVersion\":1,\"payload\":{\"carrier\":\"dhl\"}}"
  "{\"eventId\":\"$(id 4)\",\"eventType\":\"OrderPlaced\",\"occurredAt\":\"$now\",\"schemaVersion\":1,\"payload\":{\"customerId\":\"cus_4\",\"amountMinor\":400,\"currency\":\"DKK\"}}"
  "{\"eventId\":\"$(id 5)\",\"eventType\":\"OrderPlaced\",\"orderId\":\"${pp}_5\",\"occurredAt\":\"$now\",\"schemaVersion\":1,\"payload\":{\"customerId\":\"cus_5\",\"amountMinor\":500}}"
)

start_seed "$prefix"
sleep "$delay"
running=no
seed_running && running=yes
published=$SECONDS
out=$(for ((i = 1; i <= 5; i++)); do printf '%s\t%s\n' "${pp}_$i" "${bodies[i]}"; done |
  docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:9092 --topic orders.events \
    --property parse.key=true 2>&1) || {
  printf '%s\n' "$out"
  echo "kafka-console-producer failed"
  echo FAIL
  exit 1
}
echo "published 5 poison messages ${delay}s into the seed, seed still running: $running"
[[ -z $out ]] || printf '%s\n' "$out"

finish_seed || true
caught_up=never
if wait_lags; then
  caught_up=$((SECONDS - published))s
  echo "lag 0 on both groups $caught_up after the poison was published"
fi
# The console logger writes from a background queue, so the last lines can trail the offset commit.
sleep 2

verified=0
if verify "$prefix"; then
  verified=1
fi

retries "$since"
poison_retries=0
for text in "Attempt " "Dead-letter write "; do
  mapfile -t lines < <(logs worker "$since" "$text")
  for line in "${lines[@]}"; do
    [[ $line == *"for ${pp}_"* ]] && poison_retries=$((poison_retries + 1))
  done
done
echo "retry lines for poison messages=$poison_retries"

dead_letters "$prefix"
seed_dead=$dead
dead_letters "$pp"
poison_dead=$dead
notified=$(sql -c "select count(*) from notifications where $(of_prefix order_id "$pp")")
echo "notifications for $pp=$notified"

mapfile -t lettered < <(logs worker "$since" "Dead-lettered " "$ledger_category")
mapfile -t skipped < <(logs worker "$since" "Malformed message at " "$notifications_category")
mapfile -t applied < <(logs worker "$since" "Applied " "$ledger_category")

# Highest seed offset order-ledger applied on each partition.
declare -A top
re="for ${prefix}_[0-9]+ at ([0-9]+):([0-9]+)\$"
for line in "${applied[@]}"; do
  if [[ $line =~ $re ]]; then
    p=${BASH_REMATCH[1]}
    o=${BASH_REMATCH[2]}
    if [[ -z ${top[$p]:-} ]] || ((o > top[$p])); then
      top[$p]=$o
    fi
  fi
done
echo

c1=1 c3=1 c4=1 no_later=0
for ((i = 1; i <= 5; i++)); do
  key=${pp}_$i
  echo "poison $i key=$key expected=Malformed ${expected[i]}"

  mapfile -t rows < <(sql -c "select reason, detail, attempts, dlq_published_at is not null from dead_letters where order_id = '$key' order by id")
  [[ ${#rows[@]} == 1 && -z ${rows[0]} ]] && rows=()
  if ((${#rows[@]} == 0)); then
    echo "  dead_letters: none"
    c1=0
  else
    for row in "${rows[@]}"; do
      echo "  dead_letters reason|detail|attempts|dlq_published: $row"
    done
    if ((${#rows[@]} != 1)) || [[ ${rows[0]} != "Malformed|${expected[i]}|1|t" ]]; then
      c1=0
    fi
  fi

  part="" offset=""
  for line in "${lettered[@]}"; do
    if [[ $line == *"for $key at "* ]]; then
      echo "  order-ledger: $line"
      if [[ $line =~ \ at\ ([0-9]+):([0-9]+)\ as\  ]]; then
        part=${BASH_REMATCH[1]}
        offset=${BASH_REMATCH[2]}
      fi
    fi
  done
  if [[ -z $part ]]; then
    echo "  order-ledger: no Dead-lettered line"
    c3=0
    c4=0
    continue
  fi

  found=no
  for line in "${skipped[@]}"; do
    if [[ $line == "Malformed message at $part:$offset skipped:"* ]]; then
      echo "  order-notifications: $line"
      found=yes
    fi
  done
  if [[ $found == no ]]; then
    echo "  order-notifications: no skip line at $part:$offset"
    c3=0
  fi

  if [[ -z ${top[$part]:-} ]] || ((top[$part] <= offset)); then
    echo "  partition $part: poison offset $offset, no later seed event on partition $part"
    no_later=$((no_later + 1))
    c4=0
  else
    echo "  partition $part: poison offset $offset, highest seed offset applied ${top[$part]}, moved past: yes"
  fi
done
echo

c2=0 c5=0 c6=0
((poison_dead == 5)) || c1=0
((poison_retries == 0)) && c2=1
((notified == 0)) || c3=0
[[ $caught_up != never ]] && c5=1
((verified && seed_dead == 0)) && c6=1

echo "seed_running_at_publish=$running verify_ok=$verified seed_dead_letters=$seed_dead poison_dead_letters=$poison_dead poison_retries=$poison_retries poison_notifications=$notified no_later_seed_event=$no_later lag0_after_publish=$caught_up"
echo "condition 1 each poison dead-lettered as Malformed with its detail, 1 attempt, DLQ published: $c1"
echo "condition 2 no retries for poison messages: $c2 ($poison_retries)"
echo "condition 3 order-notifications skipped each, no notifications: $c3 (notifications=$notified)"
echo "condition 4 seed event applied after each poison on its partition: $c4 (no later seed event=$no_later)"
echo "condition 5 lag 0 on both groups: $c5 ($caught_up)"
echo "condition 6 seed verify OK, no seed dead letters: $c6 (verify_ok=$verified dead_letters=$seed_dead)"
if ((c1 && c2 && c3 && c4 && c5 && c6)); then
  echo PASS
  exit 0
fi
echo FAIL
exit 1
