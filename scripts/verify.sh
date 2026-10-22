#!/usr/bin/env bash
# Usage: verify.sh [count] [prefix]
# Checks the ledger state that seed.sh with the same arguments should produce.
set -euo pipefail

n=${1:-500}
prefix=${2:?prefix is required; use the one seed.sh printed}

if [[ ! $n =~ ^[1-9][0-9]*$ ]]; then
  echo "count must be a positive integer, got '$n'" >&2
  exit 2
fi
if [[ ! $prefix =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "prefix may contain only letters, digits, '_' and '-', got '$prefix'" >&2
  exit 2
fi

here=${BASH_SOURCE[0]%/*}
[[ $here != "${BASH_SOURCE[0]}" ]] || here=.
cd "$here/.."

# Must stay identical to seed.sh.
fnv() {
  local s=$1 h=-3750763034362895579 i c
  for ((i = 0; i < ${#s}; i++)); do
    printf -v c '%d' "'${s:i:1}"
    ((h ^= c, h *= 1099511628211))
  done
  printf '%016x' "$h"
}

h=$(fnv "$prefix")
event_id() {
  printf -v id '%s-%s-8%s-800%x-%012x' "${h:0:8}" "${h:8:4}" "${h:12:3}" "$1" "$2"
}

# processed_events has no order_id, so the ledger group's rows are matched by the eventIds seed.sh sent.
ids=""
for ((i = 1; i <= n; i++)); do
  event_id 1 "$i"
  ids+="('$id'::uuid),"
  event_id $((i % 2 == 0 ? 2 : 3)) "$i"
  ids+="('$id'::uuid),"
done
ids=${ids%,}

# The digit check keeps prefix "a" from matching orders of prefix "a_b".
row=$(docker compose exec -T postgres psql -U eventpulse -d eventpulse -X -q -At -v ON_ERROR_STOP=1 \
  -v p="$prefix" -v n="$n" <<SQL
with seeded as (
  select o.*, substr(o.order_id, length(:'p') + 2)::bigint as i
  from orders o
  where left(o.order_id, length(:'p') + 1) = :'p' || '_'
    and substr(o.order_id, length(:'p') + 2) ~ '^[1-9][0-9]*\$'
)
select
  (select count(*) from seeded),
  (select count(*) from seeded where i <= :n and i % 2 = 0 and status = 'Paid'),
  (select count(*) from seeded where i <= :n and i % 2 = 1 and status = 'Cancelled'),
  (select count(*) from seeded where version = 2),
  (select count(*) from processed_events
     where consumer_group = 'order-ledger' and event_id in (values $ids)),
  (select count(*) from notifications
     where left(order_id, length(:'p') + 1) = :'p' || '_'
       and substr(order_id, length(:'p') + 2) ~ '^[1-9][0-9]*\$');
SQL
)
row=${row%$'\r'}

IFS='|' read -r orders paid cancelled v2 processed notified <<<"$row"

failed=0
check() {
  if (($2 != $3)); then
    echo "FAIL $1: actual $2, expected $3"
    failed=1
  fi
}

check "orders with prefix $prefix" "$orders" "$n"
check "even orders Paid" "$paid" $((n / 2))
check "odd orders Cancelled" "$cancelled" $(((n + 1) / 2))
check "orders at version 2" "$v2" "$n"
check "processed_events for order-ledger" "$processed" $((n * 2))
check "notifications" "$notified" $((n * 2))

if ((failed)); then
  exit 1
fi
echo OK
