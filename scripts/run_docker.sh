#!/usr/bin/env bash
# Manages the clickhouse, prometheus, grafana, and redis services from the
# devcontainer compose stack. The 'app' service is left untouched (you
# presumably run cargo on the host).
set -euo pipefail

SCRIPT_DIR=".devcontainer"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
PROJECT_NAME="solana_sentinel"

# Services this script manages. Lifecycle commands act on all of them.
SERVICES=(
  clickhouse
  prometheus
  grafana
  redis-standalone
  redis-cluster
)

# Data volumes owned by the managed services. Single source of truth for both
# 'status' and 'nuke' so the two can't drift.
VOLUMES=(
  "${PROJECT_NAME}_clickhouse_data"
  "${PROJECT_NAME}_clickhouse_logs"
  "${PROJECT_NAME}_prometheus_data"
  "${PROJECT_NAME}_grafana_data"
  "${PROJECT_NAME}_redis-standalone_data"
  "${PROJECT_NAME}_redis-cluster_data"
)

dc() {
  docker compose -f "${COMPOSE_FILE}" -p "${PROJECT_NAME}" "$@"
}

# True if $1 is in the managed SERVICES list.
is_managed() {
  local needle="$1"
  for service in "${SERVICES[@]}"; do
    [[ "${service}" == "${needle}" ]] && return 0
  done
  return 1
}

# Wait for a single service to report a healthy healthcheck (if it has one).
wait_healthy() {
  local service="$1"
  echo "==> Waiting for ${service} to be healthy..."
  for _ in {1..30}; do
    status=$(dc ps --format json "${service}" 2>/dev/null | grep -o '"Health":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "")
    # Services without a healthcheck report empty status; don't block on those.
    if [[ "${status}" == "healthy" ]]; then
      echo "==> ${service} is healthy"
      return 0
    fi
    if [[ -z "${status}" ]]; then
      echo "==> ${service} has no healthcheck; assuming up"
      return 0
    fi
    sleep 2
  done
  echo "==> ${service} did not become healthy in time (continuing)"
}

cmd_up() {
  echo "==> Starting ${SERVICES[*]}..."
  # --no-deps prevents compose from also pulling in 'app' via depends_on graphs.
  # It also means a disabled service stays down even if another service in the
  # compose file declares depends_on for it.
  dc up -d --no-deps "${SERVICES[@]}"
  for service in "${SERVICES[@]}"; do
    wait_healthy "${service}"
  done
  cmd_status
}

cmd_start() {
  echo "==> Starting existing containers: ${SERVICES[*]}..."
  dc start "${SERVICES[@]}"
  cmd_status
}

cmd_stop() {
  echo "==> Stopping ${SERVICES[*]} (volumes kept)..."
  dc stop "${SERVICES[@]}"
}

cmd_down() {
  echo "==> Stopping and removing containers (volumes kept): ${SERVICES[*]}..."
  dc rm -sf "${SERVICES[@]}"
}

cmd_restart() {
  echo "==> Restarting ${SERVICES[*]}..."
  dc restart "${SERVICES[@]}"
  cmd_status
}

cmd_status() {
  echo "==> Service status:"
  dc ps "${SERVICES[@]}"
  echo
  echo "==> Volumes:"
  local filters=()
  for volume in "${VOLUMES[@]}"; do
    filters+=(--filter "name=${volume}")
  done
  docker volume ls "${filters[@]}" --format "table {{.Name}}\t{{.Driver}}"
}

cmd_client() {
  echo "==> Opening clickhouse-client (monitoring db)..."
  dc exec clickhouse clickhouse-client --user default --password password --database monitoring
}

cmd_redis_cli_standalone() {
  if ! is_managed redis-standalone; then
    echo "redis-standalone is disabled in this script; uncomment it in SERVICES to use it." >&2
    exit 1
  fi
  echo "==> Opening redis-cli..."
  dc exec redis-standalone redis-cli
}

cmd_redis_cli_cluster() {
  if ! is_managed redis-cluster; then
    echo "redis-cluster is disabled in this script; uncomment it in SERVICES to use it." >&2
    exit 1
  fi
  echo "==> Opening redis-cli (cluster mode)..."
  dc exec redis-cluster redis-cli -c
}

cmd_shell() {
  # Default to clickhouse; allow `sh prometheus` to target the other service.
  local service="${1:-clickhouse}"
  echo "==> Entering ${service} container shell..."
  dc exec "${service}" sh -c 'command -v bash >/dev/null 2>&1 && exec bash || exec sh'
}

cmd_logs() {
  # Tail all managed services by default, or a single one if named.
  if [[ $# -gt 0 ]]; then
    dc logs -f --tail=200 "$1"
  else
    dc logs -f --tail=200 "${SERVICES[@]}"
  fi
}

cmd_nuke() {
  echo "!!! This will remove the ${SERVICES[*]} containers AND their data volumes."
  echo "!!! Volumes affected: ${VOLUMES[*]}"
  read -r -p "Are you sure? [y/N] " confirm
  if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
    echo "Aborted."
    exit 1
  fi
  echo "==> Removing containers..."
  dc rm -sf "${SERVICES[@]}"
  echo "==> Removing volumes..."
  docker volume rm -f "${VOLUMES[@]}" 2>/dev/null || true
  echo "==> Done. Next 'up' will start fresh."
}

cmd_help() {
  cat <<EOF
Usage: $(basename "$0") <command>

Manages: ${SERVICES[*]}

Lifecycle:
  up         Start managed services (waits for healthchecks)
  start      Start previously-created containers
  stop       Stop containers, keep them for next 'start'
  down       Stop and remove containers, keep volumes
  restart    Restart services
  nuke       Remove containers AND data volumes [confirms]

Inspection:
  status     Show container and volume status
  logs [svc] Tail logs for all services, or just <svc> if given

Shells:
  client     Open clickhouse-client on the monitoring db
  redis-cli [standalone|cluster] Open redis-cli for the given redis service
  shell [svc] Open a shell inside <svc> (default: clickhouse)
  help       Show this message
EOF
}

main() {
  local cmd="${1:-help}"
  shift || true
  case "${cmd}" in
    up)               cmd_up "$@" ;;
    start)            cmd_start "$@" ;;
    stop)             cmd_stop "$@" ;;
    down)             cmd_down "$@" ;;
    restart)          cmd_restart "$@" ;;
    nuke)             cmd_nuke "$@" ;;
    status|ps)        cmd_status "$@" ;;
    logs)             cmd_logs "$@" ;;
    client|ch)        cmd_client "$@" ;;
    redis-cli-standalone|rcli-standalone) cmd_redis_cli_standalone "$@" ;;
    redis-cli-cluster|rcli-cluster) cmd_redis_cli_cluster "$@" ;;
    shell|sh)         cmd_shell "$@" ;;
    help|-h|--help)   cmd_help ;;
    *)
      echo "Unknown command: ${cmd}" >&2
      echo
      cmd_help
      exit 1
      ;;
  esac
}

main "$@"
