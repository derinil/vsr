#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose="${COMPOSE_FILE:-$root/docker-compose.yml}"
interval="${INTERVAL:-5}"
down_secs="${DOWN:-3}"
pause_secs="${PAUSE:-3}"
disconnect_secs="${DISCONNECT:-4}"

log() { echo "$(date '+%H:%M:%S') chaos: $*" >&2; }
nap() { sleep "$1" & wait $!; }
cid() { docker compose -f "$compose" ps -aq "$1" 2>/dev/null || true; }
dock() { docker "$@" >/dev/null 2>&1 || log "docker $* failed"; }

services="$(docker compose -f "$compose" config --services)"
network="$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$(cid "$(echo $services | cut -d' ' -f1)")")"

pick() {
  local want=$1 out="" i
  local -a pool=($services)
  while [ "$want" -gt 0 ]; do
    i=$((RANDOM % ${#pool[@]}))
    out="$out ${pool[$i]}"
    unset 'pool[i]'
    pool=("${pool[@]}")
    want=$((want - 1))
  done
  echo $out
}

primary() {
  local svc line view best="" bestview=-1
  for svc in $services; do
    line="$(docker logs "$(cid "$svc")" 2>&1 | grep 'became primary' | tail -n 1 || true)"
    view="$(echo "$line" | sed -n 's/.* view=\([0-9]*\).*/\1/p')"
    if [ -n "$view" ] && [ "$view" -gt "$bestview" ]; then
      best=$svc
      bestview=$view
    fi
  done
  echo "${best:-$(pick 1)}"
}

kill_replicas() {
  local svcs svc
  svcs="$(pick $((RANDOM % 2 + 1)))"
  for svc in $svcs; do
    log "kill $svc"
    dock kill "$(cid "$svc")"
  done
  nap "$down_secs"
  for svc in $svcs; do
    log "start $svc"
    dock start "$(cid "$svc")"
  done
}

pause_primary() {
  local svc id
  svc="$(primary)"
  id="$(cid "$svc")"
  log "pause primary $svc for ${pause_secs}s"
  dock pause "$id"
  nap "$pause_secs"
  log "unpause $svc"
  dock unpause "$id"
}

disconnect_replica() {
  local svc id
  svc="$(pick 1)"
  id="$(cid "$svc")"
  log "disconnect $svc from $network for ${disconnect_secs}s"
  dock network disconnect "$network" "$id"
  nap "$disconnect_secs"
  log "reconnect $svc"
  dock network connect --alias "$svc" "$network" "$id"
}

restore() {
  local svc id
  for svc in $services; do
    id="$(cid "$svc")"
    docker unpause "$id" >/dev/null 2>&1 || true
    docker start "$id" >/dev/null 2>&1 || true
    docker network connect --alias "$svc" "$network" "$id" >/dev/null 2>&1 || true
  done
  log "restored all replicas"
}

trap restore EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

log "started: interval=${interval}s down=${down_secs}s pause=${pause_secs}s disconnect=${disconnect_secs}s network=$network"
while :; do
  nap "$interval"
  case $((RANDOM % 3)) in
    0) kill_replicas ;;
    1) pause_primary ;;
    2) disconnect_replica ;;
  esac
done
