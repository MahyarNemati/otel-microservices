#!/usr/bin/env bash
#
# Generate traffic through the gateway and print the trace id of each request,
# so you can paste one into Grafana's "Trace and log correlation" dashboard.
#
#   ./scripts/traffic.sh            # 20 requests, mixed outcomes
#   ./scripts/traffic.sh 100        # 100 requests
#   GATEWAY=http://localhost:8080 ./scripts/traffic.sh
set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8080}"
COUNT="${1:-20}"

# A mix that produces every interesting trace shape:
#   SKU-TEA / SKU-COFFEE  -> success
#   SKU-COCOA             -> succeeds until stock runs out, then 409
#   OOS-MATCHA            -> 409 out of stock (client fault, span status Unset)
#   ERR-BROKEN            -> 500 recovered panic (span status Error + stack trace)
#   FAIL-CRATE            -> succeeds at checkout, then fails in the Node worker,
#                            so the error is on the far side of the queue
#   SKU-NOPE              -> 404 unknown SKU
SKUS=(SKU-TEA SKU-TEA SKU-COFFEE SKU-COFFEE SKU-COCOA OOS-MATCHA ERR-BROKEN FAIL-CRATE SKU-NOPE)
CUSTOMERS=(ada grace alan edsger barbara)

printf 'Sending %s requests to %s\n\n' "$COUNT" "$GATEWAY"
printf '%-6s %-12s %-6s %s\n' 'n' 'sku' 'status' 'trace_id'
printf '%-6s %-12s %-6s %s\n' '-----' '-----------' '------' '--------------------------------'

declare -A by_status=()
first_trace=""

for ((i = 1; i <= COUNT; i++)); do
  sku="${SKUS[$((RANDOM % ${#SKUS[@]}))]}"
  customer="${CUSTOMERS[$((RANDOM % ${#CUSTOMERS[@]}))]}"
  qty=$((1 + RANDOM % 3))

  # -D - writes the response headers to stdout so X-Trace-Id can be read
  # without needing the body to be valid JSON.
  response="$(curl -sS -X POST "$GATEWAY/checkout" \
    -H 'Content-Type: application/json' \
    -D - -o /dev/null -w '%{http_code}' \
    -d "{\"sku\":\"$sku\",\"quantity\":$qty,\"customer\":\"$customer\"}" 2>/dev/null || true)"

  status="$(printf '%s' "$response" | tail -n1)"
  trace="$(printf '%s' "$response" | tr -d '\r' | awk 'tolower($1) == "x-trace-id:" { print $2 }' | head -n1)"
  [[ -z "$status" ]] && status='---'
  [[ -z "$trace" ]] && trace='(none)'
  [[ -z "$first_trace" && "$trace" != '(none)' ]] && first_trace="$trace"

  by_status[$status]=$(( ${by_status[$status]:-0} + 1 ))
  printf '%-6s %-12s %-6s %s\n' "$i" "$sku" "$status" "$trace"
  sleep 0.1
done

printf '\nStatus summary:\n'
for status in $(printf '%s\n' "${!by_status[@]}" | sort); do
  printf '  %-6s %s\n' "$status" "${by_status[$status]}"
done

if [[ -n "$first_trace" ]]; then
  mkdir -p "$(dirname "${BASH_SOURCE[0]}")"
  printf '%s\n' "$first_trace" > "$(dirname "${BASH_SOURCE[0]}")/.last-trace-id"
  cat <<EOF

Next:
  Grafana      http://localhost:3000/d/otel-correlation  (paste a trace id)
  RED metrics  http://localhost:3000/d/otel-red          (click a latency exemplar)
  Tempo API    curl -s http://localhost:3200/api/traces/$first_trace | jq '.batches | length'
EOF
fi
