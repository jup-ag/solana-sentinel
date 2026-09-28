#!/usr/bin/env bash
#
# pyth_account_scan.sh
#
# Enumerates accounts owned by the Pyth oracle programs and groups them by
# account SIZE (the `space` field) — no byte decoding. Each account type is a
# fixed length, so the size distribution alone tells the kinds apart:
#   - legacy price vs product vs mapping are all different sizes
#   - the pull-oracle PriceUpdateV2 / TwapUpdate are their own distinct sizes
#
# Fetches with dataSlice length:0 (zero payload — fast, tiny response) and caps
# how many accounts are inspected per program.
#
# REQUIREMENTS: bash, curl, jq
#
# USAGE:
#   RPC_URL="https://your-endpoint" ./pyth_account_scan.sh
#   RPC_URL="https://your-endpoint" LIMIT=200 ./pyth_account_scan.sh legacy
#
# NOTE: public api.mainnet-beta.solana.com BLOCKS getProgramAccounts on these keys
# ("excluded from account secondary indexes"). Use a gPA-capable provider
# (e.g. Triton, Helius). The script never embeds a key.

set -euo pipefail

RPC_URL="https://jupiter.rpcpool.com/150a2826-9fbd-458a-8055-dfe584d30238"

# Cap how many accounts to inspect per program (these can own thousands).
LIMIT="${LIMIT:-100}"

PROGRAM_LEGACY="FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH"
PROGRAM_PUSH="pythWSnswVUd12oZpeFP8e9CVaEqJg25g1Vtc2biRsT"
PROGRAM_RECEIVER="rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ"

err()  { printf '\033[31m%s\033[0m\n' "$*" >&2; }
info() { printf '\033[36m%s\033[0m\n' "$*" >&2; }
ok()   { printf '\033[32m%s\033[0m\n' "$*" >&2; }

require() {
  command -v "$1" >/dev/null 2>&1 || { err "missing dependency: $1"; exit 1; }
}
require curl
require jq

# ---- fetch one program's accounts (NO data, just metadata) ------------------
# dataSlice length:0 returns zero payload bytes, so each entry carries only
# pubkey + space + lamports + owner — all we need to group by size.
fetch_program() {
  local program="$1"
  local body
  body=$(cat <<JSON
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getProgramAccounts",
  "params": [
    "${program}",
    {
      "encoding": "base64",
      "dataSlice": { "offset": 0, "length": 0 }
    }
  ]
}
JSON
)
  curl -s "$RPC_URL" \
    -X POST \
    -H "Content-Type: application/json" \
    -d "$body"
}

# ---- summarize the accounts of one program by size --------------------------
classify_program() {
  local label="$1"
  local program="$2"

  info ""
  info "=================================================================="
  info " ${label}"
  info " ${program}"
  info "=================================================================="

  local resp
  resp=$(fetch_program "$program")

  # RPC-level error (e.g. gPA blocked on public endpoint)
  if echo "$resp" | jq -e '.error' >/dev/null 2>&1; then
    err "RPC error for ${label}:"
    echo "$resp" | jq -r '.error.message' >&2
    err "(This endpoint likely blocks getProgramAccounts on this key. Use a gPA-capable provider.)"
    return 0
  fi

  local total
  total=$(echo "$resp" | jq '.result | length')
  ok "accounts owned by program: ${total}  (inspecting up to ${LIMIT})"
  if [[ "$total" == "0" ]]; then
    return 0
  fi

  # Group the first LIMIT accounts by `space`; count each size bucket; show one
  # example pubkey per size. Pure jq, no decoding.
  echo "$resp" | jq -r --argjson limit "$LIMIT" '
    (.result[:$limit]) as $sample
    | ($sample | length) as $n
    | "  sampled \($n) of \(.result | length) accounts; size buckets:",
      ( $sample
        | group_by(.account.space)
        | map({ space: .[0].account.space, count: length, example: .[0].pubkey })
        | sort_by(-.count)
        | .[]
        | "    count=\(.count)  space=\(.space) bytes   e.g. \(.example)"
      ),
      "  pubkeys (\($n)):",
      ( $sample[] | "    \(.pubkey)  (space=\(.account.space))" )
  '
}

# ---- main -------------------------------------------------------------------
target="${1:-all}"

info "RPC endpoint: ${RPC_URL}"
info "Grouping accounts by size (no payload fetched); LIMIT=${LIMIT} per program."

case "$target" in
  legacy)   classify_program "LEGACY ORACLE"   "$PROGRAM_LEGACY"   ;;
  push)     classify_program "PUSH ORACLE"     "$PROGRAM_PUSH"     ;;
  receiver) classify_program "SOLANA RECEIVER" "$PROGRAM_RECEIVER" ;;
  all)
    classify_program "LEGACY ORACLE"   "$PROGRAM_LEGACY"
    classify_program "PUSH ORACLE"     "$PROGRAM_PUSH"
    classify_program "SOLANA RECEIVER" "$PROGRAM_RECEIVER"
    ;;
  *)
    err "unknown target '$target' (use: all | legacy | push | receiver)"
    exit 1
    ;;
esac

ok ""
ok "Done."