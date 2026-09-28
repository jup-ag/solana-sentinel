#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Snapshot what came in from the CLI before .env can stomp it
cli_rules=${RULES_FILE:-}

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

[[ -n $cli_rules ]] && RULES_FILE=$cli_rules

: "${RULES_FILE:=rules.toml}"
: "${RUST_LOG:=info,solana_sentinel=debug}"
: "${RUST_BACKTRACE:=1}"
export RULES_FILE RUST_LOG RUST_BACKTRACE
exec cargo run --bin solana_sentinel "$@"
