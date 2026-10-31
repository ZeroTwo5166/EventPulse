#!/usr/bin/env bash
# Usage: stop-redis.sh [seconds]
# Failure test 6. Seeds a fresh prefix, docker stops redis 5 s into the seed for the given number of seconds
# (default 30), starts it again and waits for PING. Then sends bursts of 250 OrderPlaced requests under one API
# key, each under its own fresh prefix, until one gets a 429 or 6 bursts have run, 10 s apart. 250 requests take
# about 5 s, so a burst spans at most two 10 s windows and one of them must see more than the limit of 100.
# Pass, all of:
#   1. Redis answers PING within 60 s of docker start.
#   2. The seed gets 202 for every request and nothing else: the API fails open.
#   3. The API logs at least one "ratelimit bypassed" line.
#   4. Both groups reach lag 0.
#   5. verify.sh prints OK.
#   6. dead_letters has no rows for the prefix.
#   7. A burst that starts within 60 s of PONG gets at least one 429: limiting resumes without an API restart.
set -euo pipefail

here=${BASH_SOURCE[0]%/*}
[[ $here != "${BASH_SOURCE[0]}" ]] || here=.
. "$here/lib.sh"

outage=${1:-30}
if [[ ! $outage =~ ^[1-9][0-9]*$ ]]; then
  echo "seconds must be a positive integer, got '$outage'" >&2
  exit 2
fi

delay=5
ready_timeout=60
size=250
bursts=6
gap=10
resume_timeout=60
api=${EVENTPULSE_API:-http://localhost:8080}
redis=$(container redis)
rate_category=EventPulse.Api.RateLimit
stopped=0

# Replaces lib.sh's trap, so it also removes the seed output. An interrupted run must not leave Redis down.
cleanup() {
  if ((stopped)); then
    docker start "$redis" >/dev/null || true
  fi
  rm -f "$seed_out"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Posts size OrderPlaced events for the prefix under one API key, eventIds built as seed.sh builds them.
# Prints one status code per request.
burst() {
  local p=$1 h i id now
  h=$(fnv "$p")
  now=$(utc)
  for ((i = 1; i <= size; i++)); do
    printf -v id '%s-%s-8%s-8001-%012x' "${h:0:8}" "${h:8:4}" "${h:12:3}" "$i"
    printf -- '-H\0X-Api-Key: %s\0--data-raw\0{"eventId":"%s","eventType":"OrderPlaced","orderId":"%s_%d","occurredAt":"%s","schemaVersion":1,"payload":{"customerId":"cus_%d","amountMinor":%d,"currency":"DKK"}}\0' \
      "$p" "$id" "$p" "$i" "$now" "$i" $((i * 100))
  done | xargs -0 -n 4 -P 8 curl -s -o /dev/null -w '%{http_code}\n' --max-time 30 \
    -H 'Content-Type: application/json' "$api/events" || true
}

prefix=$(fresh redis)
since=$(utc)
echo "outage=${outage}s prefix=$prefix since=$since"

start_seed "$prefix"
sleep "$delay"
running=no
seed_running && running=yes
stop_since=$(utc)
stopping_at=$SECONDS
stopped=1
docker stop "$redis" >/dev/null
stopped_at=$SECONDS
echo "redis stopped ${delay}s into the seed in $((stopped_at - stopping_at))s, seed still running: $running"
sleep "$outage"
start_since=$(utc)
docker start "$redis" >/dev/null
stopped=0
started_at=$SECONDS
echo "redis started after $((started_at - stopped_at))s down"

ready=never
while ((SECONDS - started_at < ready_timeout)); do
  pong=$(docker compose exec -T redis redis-cli ping 2>/dev/null) || true
  if [[ ${pong%$'\r'} == PONG ]]; then
    ready=$((SECONDS - started_at))s
    break
  fi
  sleep 1
done
pong_at=$SECONDS
echo "redis PONG $ready after docker start"
if [[ $ready == never ]]; then
  echo "redis did not answer PING within ${ready_timeout}s"
  echo FAIL
  exit 1
fi

resumed=never
for ((b = 1; b <= bursts; b++)); do
  limit_prefix=$prefix-limit$b
  at=$((SECONDS - pong_at))
  ok=0 limited=0 other=0
  while read -r code; do
    code=${code%$'\r'}
    [[ -n $code ]] || continue
    if [[ $code == 202 ]]; then
      ok=$((ok + 1))
    elif [[ $code == 429 ]]; then
      limited=$((limited + 1))
    else
      other=$((other + 1))
    fi
  done < <(burst "$limit_prefix")
  echo "burst $b prefix=$limit_prefix started ${at}s after PONG, took $((SECONDS - pong_at - at))s: 202=$ok 429=$limited other=$other"
  if ((limited > 0)); then
    resumed=$at
    break
  fi
  if ((b < bursts)); then
    sleep "$gap"
  fi
done
delay_text=never
if [[ $resumed == never ]]; then
  echo "reconnect delay: no burst got a 429"
else
  delay_text=${resumed}s
  echo "reconnect delay: first burst with a 429 started $delay_text after PONG"
fi

finish_seed || true
seed_ok=0 seed_bad=0
if [[ $(<"$seed_out") =~ \ 202=([0-9]+)\ non-202=([0-9]+) ]]; then
  seed_ok=${BASH_REMATCH[1]}
  seed_bad=${BASH_REMATCH[2]}
fi

caught_up=never
caught_up_stop=never
if wait_lags; then
  caught_up=$((SECONDS - started_at))s
  caught_up_stop=$((SECONDS - stopped_at))s
  echo "lag 0 on both groups $caught_up after start, $caught_up_stop after stop"
fi
# The console logger writes from a background queue, so the last lines can trail the response.
sleep 2

verified=0
if verify "$prefix"; then
  verified=1
fi

bypassed=0
for reason in disconnected "slower than" failed; do
  mapfile -t lines < <(logs api "$stop_since" "ratelimit bypassed, Redis $reason" "$rate_category")
  bypassed=$((bypassed + ${#lines[@]}))
  report "ratelimit bypassed since stop, Redis $reason" "${lines[@]}"
done
bypassed_after=0
for reason in disconnected "slower than" failed; do
  mapfile -t lines < <(logs api "$start_since" "ratelimit bypassed, Redis $reason" "$rate_category")
  bypassed_after=$((bypassed_after + ${#lines[@]}))
  report "ratelimit bypassed since docker start, Redis $reason" "${lines[@]}"
done
dead_letters "$prefix"
echo

c1=0 c2=0 c3=0 c4=0 c5=0 c6=0 c7=0
[[ $ready != never ]] && c1=1
((seed_ok == orders * 2 && seed_bad == 0)) && c2=1
((bypassed > 0)) && c3=1
[[ $caught_up != never ]] && c4=1
((verified)) && c5=1
((dead == 0)) && c6=1
[[ $resumed != never ]] && ((resumed <= resume_timeout)) && c7=1

echo "outage=$outage seed_202=$seed_ok seed_non_202=$seed_bad verify_ok=$verified dead_letters=$dead bypassed_since_stop=$bypassed bypassed_since_start=$bypassed_after pong_after_start=$ready lag0_after_start=$caught_up lag0_after_stop=$caught_up_stop"
echo "reconnect_delay=$delay_text"
echo "condition 1 redis PONG within ${ready_timeout}s: $c1 ($ready)"
echo "condition 2 seed all 202: $c2 (202=$seed_ok non-202=$seed_bad)"
echo "condition 3 ratelimit bypassed logged: $c3 ($bypassed)"
echo "condition 4 lag 0 on both groups: $c4 ($caught_up after start)"
echo "condition 5 verify OK: $c5"
echo "condition 6 no dead letters: $c6 ($dead)"
echo "condition 7 429 within ${resume_timeout}s of PONG: $c7 (first at $delay_text)"
if ((c1 && c2 && c3 && c4 && c5 && c6 && c7)); then
  echo PASS
  exit 0
fi
echo FAIL
exit 1
