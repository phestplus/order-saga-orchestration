#!/usr/bin/env bash
# End-to-end proof that the saga compensates correctly.
#
# Runs against the real compose stack (NATS plus five services) over HTTP,
# and asserts on every outcome rather than printing them, so this doubles
# as the CI gate.
#
# The scenarios walk the failure surface deliberately: a failure at each
# step in turn, a failure whose compensation also fails, and finally a
# conservation check that proves stock was genuinely returned rather than
# merely reported as returned.
set -euo pipefail

cd "$(dirname "$0")/.."

ORDER_URL="${ORDER_URL:-http://127.0.0.1:8080}"
NOTIFY_URL="${NOTIFY_URL:-http://127.0.0.1:8081}"
COMPOSE="${COMPOSE:-docker compose}"

pass=0
fail=0

log()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }
ok()   { printf '    \033[32mok\033[0m   %s\n' "$*"; pass=$((pass + 1)); }
bad()  { printf '    \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail + 1)); }

expect() {
  local label="$1" actual="$2" want="$3"
  if [ "$actual" = "$want" ]; then
    ok "$label = $actual"
  else
    bad "$label = '$actual', want '$want'"
  fi
}

wait_for() {
  local url="$1" name="$2" waited=0
  while [ "$waited" -lt 120 ]; do
    if curl -sf --max-time 3 "$url/healthz" >/dev/null 2>&1; then
      info "$name is up"
      return 0
    fi
    sleep 2; waited=$((waited + 2))
  done
  echo "$name never became healthy at $url" >&2
  return 1
}

# Places an order and echoes "<http_status> <body>".
place_order() {
  local body="$1" out status
  out="$(curl -s -w '\n%{http_code}' --max-time 20 \
    -X POST "$ORDER_URL/orders" \
    -H 'Content-Type: application/json' \
    -d "$body")"
  status="$(printf '%s' "$out" | tail -n1)"
  printf '%s\t%s' "$status" "$(printf '%s' "$out" | sed '$d')"
}

if [ "${SKIP_COMPOSE:-0}" != "1" ]; then
  log "Starting the stack (NATS + 5 services)"
  $COMPOSE up -d --build
  cleanup() {
    echo ""
    echo "Stack is still running. Inspect it with '$COMPOSE logs -f',"
    echo "or tear it down with '$COMPOSE down -v'."
  }
  trap cleanup EXIT
fi

log "Waiting for services"
wait_for "$ORDER_URL" order-service
wait_for "$NOTIFY_URL" notification-service

# ------------------------------------------------------------ scenario 1

log "1. Happy path: every step succeeds"
IFS=$'\t' read -r status body <<<"$(place_order '{"id":"ok-1","sku":"WIDGET","qty":1,"amountCents":2500}')"
expect "http status" "$status" "201"
expect "saga status" "$(jq -r .status <<<"$body")" "completed"
expect "completed steps" "$(jq -rc .completed <<<"$body")" '["inventory","payment","shipping"]'
expect "compensations" "$(jq -r '.compensated // [] | length' <<<"$body")" "0"

# ------------------------------------------------------------ scenario 2

log "2. Failure on the first step: nothing to undo"
IFS=$'\t' read -r status body <<<"$(place_order '{"id":"fail-inv","sku":"WIDGET","qty":1,"amountCents":2500,"failAt":"inventory"}')"
expect "http status" "$status" "409"
expect "saga status" "$(jq -r .status <<<"$body")" "compensated"
expect "failed step" "$(jq -r .failedStep <<<"$body")" "inventory"
# Nothing had been applied yet, so compensating anything here would be a bug.
expect "compensations" "$(jq -r '.compensated // [] | length' <<<"$body")" "0"

# ------------------------------------------------------------ scenario 3

log "3. Failure mid-saga: the completed step is undone"
IFS=$'\t' read -r status body <<<"$(place_order '{"id":"fail-pay","sku":"WIDGET","qty":1,"amountCents":2500,"failAt":"payment"}')"
expect "http status" "$status" "409"
expect "saga status" "$(jq -r .status <<<"$body")" "compensated"
expect "failed step" "$(jq -r .failedStep <<<"$body")" "payment"
expect "compensated" "$(jq -rc .compensated <<<"$body")" '["inventory"]'

# ------------------------------------------------------------ scenario 4

log "4. Failure on the last step: both prior steps undone, in reverse"
IFS=$'\t' read -r status body <<<"$(place_order '{"id":"fail-ship","sku":"WIDGET","qty":1,"amountCents":2500,"failAt":"shipping"}')"
expect "http status" "$status" "409"
expect "saga status" "$(jq -r .status <<<"$body")" "compensated"
# Reverse order is the assertion that matters: payment is undone before
# inventory, because later steps can depend on earlier ones.
expect "compensated (reverse order)" "$(jq -rc .compensated <<<"$body")" '["payment","inventory"]'

# ------------------------------------------------------------ scenario 5

log "5. A compensation itself fails: the saga is stuck, not silently 'rolled back'"
IFS=$'\t' read -r status body <<<"$(place_order '{"id":"stuck-1","sku":"WIDGET","qty":1,"amountCents":2500,"failAt":"shipping","failCompensationAt":"payment"}')"
expect "http status" "$status" "500"
expect "saga status" "$(jq -r .status <<<"$body")" "stuck"
expect "failed compensation" "$(jq -r '.compensationFailures[0].step' <<<"$body")" "payment"
# The refund failed, but inventory must still have been released: undoing
# two of three effects beats abandoning the rollback entirely.
expect "still compensated inventory" "$(jq -rc .compensated <<<"$body")" '["inventory"]'

# ------------------------------------------------------------ scenario 6

log "6. Conservation: stock released by a rollback is genuinely available again"
info "GADGET stock is 5. Reserve all 5 in an order that fails at shipping,"
info "then place a second order for all 5. It can only succeed if the"
info "first order's reservation was actually returned."
IFS=$'\t' read -r status body <<<"$(place_order '{"id":"gadget-rollback","sku":"GADGET","qty":5,"amountCents":100,"failAt":"shipping"}')"
expect "first order rolled back" "$(jq -r .status <<<"$body")" "compensated"

IFS=$'\t' read -r status body <<<"$(place_order '{"id":"gadget-retry","sku":"GADGET","qty":5,"amountCents":100}')"
expect "second order succeeds on returned stock" "$(jq -r .status <<<"$body")" "completed"

# A third order must now fail: the 5 units are legitimately gone.
IFS=$'\t' read -r status body <<<"$(place_order '{"id":"gadget-exhausted","sku":"GADGET","qty":5,"amountCents":100}')"
expect "third order refused, stock exhausted" "$(jq -r .failureReason <<<"$body")" "reserve: out of stock"

# ------------------------------------------------------------ scenario 7

log "7. The notification service saw every outcome"
sleep 2 # events are published asynchronously
notifications="$(curl -sf --max-time 10 "$NOTIFY_URL/notifications")"
expect "critical notification for the stuck saga" \
  "$(jq -r '[.notifications[] | select(.orderId=="stuck-1")][0].severity' <<<"$notifications")" "critical"
expect "warning for the rolled-back saga" \
  "$(jq -r '[.notifications[] | select(.orderId=="fail-pay")][0].severity' <<<"$notifications")" "warning"
expect "info for the successful saga" \
  "$(jq -r '[.notifications[] | select(.orderId=="ok-1")][0].severity' <<<"$notifications")" "info"

# ----------------------------------------------------------------- result

log "RESULT"
info "assertions passed: $pass"
info "assertions failed: $fail"
if [ "$fail" -ne 0 ]; then
  printf '\n\033[31mFAIL: %d assertion(s) failed.\033[0m\n' "$fail"
  exit 1
fi
printf '\n\033[32mPASS: all %d assertions held. Compensation runs in reverse, partial rollback is reported as stuck, and released stock is genuinely reusable.\033[0m\n' "$pass"
