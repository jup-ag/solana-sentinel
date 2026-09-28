#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Load .env if present (without overriding already-exported vars)
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

: "${RUST_LOG:=info,solana_sentinel=debug}"
export RUST_LOG

exec cargo test -p alerting -- --nocapture "$@"
