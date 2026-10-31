#!/usr/bin/env bash
# Usage: pause-postgres.sh seconds
# Failure test 4. Seeds a fresh prefix, docker pauses postgres 5 s into the seed for the given number of seconds,
# unpauses it, waits for lag 0 in both consumer groups and runs verify.sh. A paused Postgres accepts no work, so
# every attempt fails on the 2 s connection or command timeout and the consumers retry in place.
# Pass, for 10 s or less: verify.sh prints OK and dead_letters has no rows for the prefix. Longer pauses are only
# reported: the ledger consumer gives up on a message after about 28 s and dead-letters it, so verify.sh can fail.
set -euo pipefail

here=${BASH_SOURCE[0]%/*}
[[ $here != "${BASH_SOURCE[0]}" ]] || here=.
. "$here/lib.sh"

pause=${1:-}
if [[ ! $pause =~ ^[1-9][0-9]*$ ]]; then
  echo "seconds must be a positive integer, got '$pause'" >&2
  exit 2
fi

delay=5
postgres=$(container postgres)
paused=0

# Replaces lib.sh's trap, so it also removes the seed output. An interrupted run must not leave Postgres frozen.
cleanup() {
  if ((paused)); then
    docker unpause "$postgres" >/dev/null || true
  fi
  rm -f "$seed_out"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

prefix=$(fresh pause)
since=$(utc)
echo "pause=${pause}s prefix=$prefix since=$since"

start_seed "$prefix"
sleep "$delay"
running=no
seed_running && running=yes
docker pause "$postgres" >/dev/null
paused=1
paused_at=$SECONDS
echo "postgres paused ${delay}s into the seed, seed still running: $running"
sleep "$pause"
docker unpause "$postgres" >/dev/null
paused=0
unpaused_at=$SECONDS
echo "postgres unpaused after $((unpaused_at - paused_at))s"

finish_seed || true
caught_up=never
if wait_lags; then
  caught_up=$((SECONDS - unpaused_at))s
  echo "lag 0 on both groups $caught_up after unpause, $((SECONDS - paused_at))s after pause"
fi
# The console logger writes from a background queue, so the last lines can trail the offset commit.
sleep 2

verified=0
if verify "$prefix"; then
  verified=1
fi

retries "$since"
dead_letters "$prefix"
echo

echo "pause=$pause verify_ok=$verified dead_letters=$dead retries_order_ledger=$retries_ledger retries_order_notifications=$retries_notifications retries_total=$((retries_ledger + retries_notifications)) dead_letter_write_retries=$dead_letter_writes lag0_after_unpause=$caught_up"
if ((pause > 10)); then
  echo "REPORT (no pass condition for ${pause}s)"
  exit 0
fi
if ((verified && dead == 0)); then
  echo PASS
  exit 0
fi
echo FAIL
exit 1
