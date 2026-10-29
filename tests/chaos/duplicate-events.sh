#!/usr/bin/env bash
# Usage: duplicate-events.sh
# Failure test 2. Seeds a fresh prefix, waits for lag 0 in both consumer groups, then runs seed.sh again with
# the same prefix, which resends the same 1,000 events under the same eventIds, and waits again.
# Pass: verify.sh prints OK, processed_events has 1,000 rows for the prefix, not 2,000, and each consumer
# group logs 1,000 "Duplicate ... skipped" lines during the replay, one per resent event.
set -euo pipefail

here=${BASH_SOURCE[0]%/*}
[[ $here != "${BASH_SOURCE[0]}" ]] || here=.
. "$here/lib.sh"

prefix=$(fresh dup)
h=$(fnv "$prefix")
echo "prefix=$prefix"

echo "first seed since=$(utc)"
scripts/seed.sh "$orders" "$prefix" || true
wait_lags || true

since=$(utc)
echo "replay since=$since"
scripts/seed.sh "$orders" "$prefix" || true
wait_lags || true
# The console logger writes from a background queue, so the last lines can trail the offset commit.
sleep 2

verified=0
if verify "$prefix"; then
  verified=1
fi

# processed_events has no order_id, so the prefix's rows are found by the prefix hash that starts every
# eventId seed.sh sends.
processed=$(sql -c "select count(*) from processed_events where event_id::text like '${h:0:8}-${h:8:4}-8${h:12:3}-%'")
echo "processed_events for $prefix=$processed"

mapfile -t lines < <(logs worker "$since" "Duplicate " "$ledger_category")
ledger=${#lines[@]}
report "duplicates order-ledger during replay" "${lines[@]}"
mapfile -t lines < <(logs worker "$since" "Duplicate " "$notifications_category")
notifications=${#lines[@]}
report "duplicates order-notifications during replay" "${lines[@]}"
echo

echo "verify_ok=$verified processed_events=$processed duplicates_order_ledger=$ledger duplicates_order_notifications=$notifications"
expected=$((orders * 2))
if ((verified && processed == expected && ledger == expected && notifications == expected)); then
  echo PASS
  exit 0
fi
echo FAIL
exit 1
