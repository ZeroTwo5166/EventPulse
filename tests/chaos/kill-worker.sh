#!/usr/bin/env bash
# Usage: kill-worker.sh [cycles]
# Failure test 1. Each cycle seeds a fresh prefix, kills the worker with docker kill (SIGKILL) at a random
# point 2-13 s into the seed, starts it again 5 s later, waits for lag 0 in both consumer groups and runs
# verify.sh. Cycles are separate seeds rather than repeated kills in one seed: the restarted worker gets no
# partitions until the killed member's session times out, which takes longer than a seed.
# Pass: verify.sh prints OK in every cycle. The test shows the ledger stays correct under a kill.
# The duplicate and open-transaction EOF counts are printed as evidence, not required: a duplicate is
# logged only when the kill lands between the Postgres commit and the offset commit, a gap of a few ms,
# and an EOF only when it lands inside a transaction, so a cycle can pass and show neither.
set -euo pipefail

here=${BASH_SOURCE[0]%/*}
[[ $here != "${BASH_SOURCE[0]}" ]] || here=.
. "$here/lib.sh"

cycles=${1:-1}
if [[ ! $cycles =~ ^[1-9][0-9]*$ ]]; then
  echo "cycles must be a positive integer, got '$cycles'" >&2
  exit 2
fi

worker=$(container worker)
verified=0
ledger=0
notifications=0
eof=0

for ((c = 1; c <= cycles; c++)); do
  prefix=$(fresh kill)-$c
  since=$(utc)
  echo "cycle $c/$cycles prefix=$prefix since=$since"

  start_seed "$prefix"
  delay=$((2 + RANDOM % 12))
  sleep "$delay"
  running=no
  seed_running && running=yes
  docker kill "$worker" >/dev/null
  echo "worker killed ${delay}s into the seed, seed still running: $running"
  sleep 5
  docker start "$worker" >/dev/null
  echo "worker started after 5s down"

  finish_seed || true
  wait_lags || true
  if verify "$prefix"; then
    verified=$((verified + 1))
  fi

  mapfile -t lines < <(logs worker "$since" "Duplicate " "$ledger_category")
  ledger=$((ledger + ${#lines[@]}))
  report "duplicates order-ledger" "${lines[@]}"
  mapfile -t lines < <(logs worker "$since" "Duplicate " "$notifications_category")
  notifications=$((notifications + ${#lines[@]}))
  report "duplicates order-notifications" "${lines[@]}"
  mapfile -t lines < <(logs postgres "$since" "unexpected EOF on client connection with an open transaction")
  eof=$((eof + ${#lines[@]}))
  report "postgres open-transaction EOF" "${lines[@]}"
  echo
done

echo "cycles=$cycles verify_ok=$verified duplicates_order_ledger=$ledger duplicates_order_notifications=$notifications postgres_open_transaction_eof=$eof"
if ((verified == cycles)); then
  echo PASS
  exit 0
fi
echo FAIL
exit 1
