#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
src="${SRC:-$root}"
build="$root/build"
compose="$root/docker-compose.yml"
shim="$build/vsrmaelstrom"
maelstrom="$build/maelstrom/maelstrom"
nodes="${NODES:-3}"
concurrency="${CONCURRENCY:-4n}"
rate="${RATE:-100}"
time_limit="${TIME:-60}"
chaos_on="${CHAOS:-1}"
cluster="127.0.0.1:10188,127.0.0.1:10189,127.0.0.1:10190,127.0.0.1:10191,127.0.0.1:10192"
chaos_pid=""

log() { echo "e2e: $*" >&2; }

stop_chaos() {
  if [[ -n "$chaos_pid" ]]; then
    kill "$chaos_pid" 2>/dev/null || true
    wait "$chaos_pid" 2>/dev/null || true
    chaos_pid=""
    log "chaos stopped"
  fi
}

finish() {
  local status=$?
  stop_chaos
  docker compose -f "$compose" logs --no-color > "$build/e2e-logs.txt" 2>&1 || true
  docker compose -f "$compose" down -v >/dev/null 2>&1 || true
  exit "$status"
}
trap finish EXIT

ensure_docker() {
  docker info >/dev/null 2>&1 && return 0
  [[ "$(uname)" == Darwin ]] || { log "docker daemon not running"; exit 1; }
  log "starting Docker Desktop"
  open -a Docker
  local deadline=$((SECONDS + 180))
  until docker info >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then log "docker daemon not available"; exit 1; fi
    sleep 2
  done
}

probe() {
  printf '%s\n%s\n' \
    '{"src":"c0","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1"]}}' \
    '{"src":"c0","dest":"n1","body":{"type":"write","msg_id":2,"key":-1,"value":0}}' \
    | VSR_CLUSTER="$cluster" "$shim" -timeout 3s \
    | grep '"type":"write_ok"' >/dev/null
}

wait_ready() {
  local deadline=$((SECONDS + 120))
  until probe; do
    if (( SECONDS >= deadline )); then log "cluster not ready"; return 1; fi
    sleep 1
  done
  log "cluster ready"
}

mkdir -p "$build"
"$root/scripts/fetch-maelstrom.sh"
ensure_docker
docker build -t vsr:latest "$src"
(cd "$src" && go build -o "$shim" ./cmd/vsrmaelstrom)

docker compose -f "$compose" down -v
docker compose -f "$compose" up -d --no-build
wait_ready

if [[ "$chaos_on" == 1 ]]; then
  "$root/scripts/chaos.sh" 2> "$build/e2e-chaos.txt" &
  chaos_pid=$!
  log "chaos started (pid $chaos_pid, log $build/e2e-chaos.txt)"
fi

status=0
(
  cd "$build"
  VSR_CLUSTER="$cluster" "$maelstrom" test -w lin-kv --bin "$shim" \
    --node-count "$nodes" --concurrency "$concurrency" --rate "$rate" --time-limit "$time_limit"
) 2>&1 | tee "$build/e2e-maelstrom.txt" || status=$?

stop_chaos
log "maelstrom exit status $status"
exit "$status"
