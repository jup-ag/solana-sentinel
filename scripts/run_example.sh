#!/usr/bin/env bash
# Runs an example deployment with its own config and rules.
#   scripts/run_example.sh spl-token-monitor          # run it
#   scripts/run_example.sh spl-token-monitor check    # validate config only, connects nowhere
# Config: $CONFIG_FILE, else ./config.toml, else config.example.toml. Rules: always the example's.
set -euo pipefail

cd "$(dirname "$0")/.."

example="${1:-}"
if [[ -z "$example" || ! -d "examples/$example" ]]; then
  echo "usage: $0 <example> [check]" >&2
  echo "examples: $(ls examples | grep -v README | tr '\n' ' ')" >&2
  exit 1
fi
shift

: "${RUST_LOG:=info}"
export RUST_LOG
# The root config.toml (gitignored, real endpoints) wins; config.example.toml is a template.
if [[ -z "${CONFIG_FILE:-}" ]]; then
  if [[ -f config.toml ]]; then CONFIG_FILE=config.toml; else CONFIG_FILE=config.example.toml; fi
fi
export CONFIG_FILE
export RULES_FILE="examples/$example/rules"

exec cargo run -p "$example-example" -- "$@"
