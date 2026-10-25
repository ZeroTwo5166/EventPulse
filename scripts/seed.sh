#!/usr/bin/env bash
# Usage: seed.sh [count] [prefix]
# Posts OrderPlaced for orders {prefix}_1..{prefix}_N, then PaymentCompleted for even i
# and OrderCancelled for odd i. Same prefix, same eventIds, so a rerun is a pure replay.
set -euo pipefail

n=${1:-500}
prefix=${2:-$(printf '%(%s)T' -1)}
api=${EVENTPULSE_API:-http://localhost:8080}

if [[ ! $n =~ ^[1-9][0-9]*$ ]]; then
  echo "count must be a positive integer, got '$n'" >&2
  exit 2
fi
# Restricted so the prefix can go into JSON and SQL without escaping.
if [[ ! $prefix =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "prefix may contain only letters, digits, '_' and '-', got '$prefix'" >&2
  exit 2
fi

# FNV-1a 64; bash arithmetic wraps at 64 bits, which is what the hash needs.
fnv() {
  local s=$1 h=-3750763034362895579 i c
  for ((i = 0; i < ${#s}; i++)); do
    printf -v c '%d' "'${s:i:1}"
    ((h ^= c, h *= 1099511628211))
  done
  printf '%016x' "$h"
}

# Version 8 (custom) UUID: 60 bits of prefix hash, type code, then i in the last group.
h=$(fnv "$prefix")
event_id() {
  printf -v id '%s-%s-8%s-800%x-%012x' "${h:0:8}" "${h:8:4}" "${h:12:3}" "$1" "$2"
}

now=$(TZ=UTC printf '%(%Y-%m-%dT%H:%M:%SZ)T' -1)

# Four curl arguments per event. seq is the event's position in the run; each API key
# carries 50 events, half the API's limit of 100 per 10 s, however fast the seed runs.
envelope() {
  local seq=$1 type=$2 code=$3 i=$4 payload=$5 id
  event_id "$code" "$i"
  printf -- '-H\0X-Api-Key: seed-%d\0--data-raw\0{"eventId":"%s","eventType":"%s","orderId":"%s_%d","occurredAt":"%s","schemaVersion":1,"payload":%s}\0' \
    $((seq / 50)) "$id" "$type" "$prefix" "$i" "$now" "$payload"
}

placed() {
  local i
  for ((i = 1; i <= n; i++)); do
    envelope $((i - 1)) OrderPlaced 1 "$i" "{\"customerId\":\"cus_$i\",\"amountMinor\":$((i * 100)),\"currency\":\"DKK\"}"
  done
}

settled() {
  local i
  for ((i = 1; i <= n; i++)); do
    if ((i % 2 == 0)); then
      envelope $((n + i - 1)) PaymentCompleted 2 "$i" "{\"paymentId\":\"pay_${prefix}_$i\",\"amountMinor\":$((i * 100))}"
    else
      envelope $((n + i - 1)) OrderCancelled 3 "$i" '{"reason":"customer_request"}'
    fi
  done
}

post() {
  xargs -0 -n 4 -P 8 curl -s -o /dev/null -w '%{http_code}\n' --max-time 30 \
    -H 'Content-Type: application/json' "$api/events" || true
}

# The API returns 202 only after Kafka acknowledges, so once this phase ends every
# OrderPlaced is in the log ahead of the event that settles it.
codes=$({ placed | post; settled | post; })

ok=0
bad=0
while read -r code; do
  # A native Windows curl writes CRLF into the pipe.
  code=${code%$'\r'}
  [[ -n $code ]] || continue
  if [[ $code == 202 ]]; then ((++ok)); else ((++bad)); fi
done <<<"$codes"

echo "seed prefix=$prefix orders=$n events=$((n * 2)) 202=$ok non-202=$bad"
((bad == 0))
