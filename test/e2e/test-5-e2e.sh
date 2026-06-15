#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_scheduler

WF_ID=""
SVC_WF_ID=""
CLEANUP_IDS=""

cleanup() {
  echo ""
  echo "=== Cleanup ==="
  for id in $CLEANUP_IDS; do
    curl -sf -X DELETE "$API/workflows/$id" >/dev/null 2>&1 && echo "  Deleted workflow $id"
  done
  echo "  Cleanup done"
}

trap cleanup EXIT

ECHO_PIPELINE="$SCRIPT_DIR/../../schemas/examples/echo-pipeline.json"
ECHO_SVC_PIPELINE="$SCRIPT_DIR/../../schemas/examples/echo-service-pipeline.json"

section "E2E A: submit echo-pipeline"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d @"$ECHO_PIPELINE")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')

if [ "$HTTP_CODE" != "201" ]; then
  echo "  FAIL: could not create echo-pipeline (HTTP $HTTP_CODE)"
  echo "  Response: $BODY"
  FAILED=$((FAILED + 1))
  exit 1
fi

WF_ID=$(echo "$BODY" | jq -r '.id')
CLEANUP_IDS="$WF_ID"

check "echo-pipeline created (201)" "201" "$HTTP_CODE"
check "echo-pipeline status is Running" "Running" "$(echo "$BODY" | jq -r '.status')"
check "echo-pipeline has 4 tasks" "4" "$(echo "$BODY" | jq -r '[.nodes[] | select(.type=="task")] | length')"
check "echo-pipeline has 3 tiers" "3" "$(echo "$BODY" | jq -r '.dag.tiers | length')"

section "E2E A: verify tier-0 tasks deployed"
TIER0_COUNT=$(echo "$BODY" | jq -r '[.nodes[] | select(.tier==0 and .operatorResourceId != null)] | length')
check "tier-0 tasks have operatorResourceId" "2" "$TIER0_COUNT"

TIER_GT0_NULL=$(echo "$BODY" | jq -r '[.nodes[] | select(.tier>0 and .operatorResourceId == null)] | length')
check "tier>0 tasks NOT deployed yet" "2" "$TIER_GT0_NULL"

section "E2E A: wait for echo-pipeline to complete"
wait_for "echo-pipeline Succeeded" "curl -sf '$API/workflows/$WF_ID' | jq -r '.status'" "Succeeded" 120

section "E2E A: verify final state"
RESP=$(curl -sf "$API/workflows/$WF_ID")
check "workflow status is Succeeded" "Succeeded" "$(echo "$RESP" | jq -r '.status')"

ALL_SUCCEEDED=$(echo "$RESP" | jq -r '[.nodes[] | select(.type=="task" and .status=="Succeeded")] | length')
TOTAL_TASKS=$(echo "$RESP" | jq -r '[.nodes[] | select(.type=="task")] | length')
check "all tasks Succeeded" "$TOTAL_TASKS" "$ALL_SUCCEEDED"

section "E2E A: verify events endpoint"
EVENTS_RESP=$(curl -sf "$API/workflows/$WF_ID/events")
EVENT_COUNT=$(echo "$EVENTS_RESP" | jq -r '.data | length')
check "events exist" "true" "$([ "$EVENT_COUNT" -gt 0 ] && echo true || echo false)"

EVENTS_LIMIT_RESP=$(curl -sf "$API/workflows/$WF_ID/events?limit=3")
LIMIT_COUNT=$(echo "$EVENTS_LIMIT_RESP" | jq -r '.data | length')
check "events limit works" "true" "$([ "$LIMIT_COUNT" -le 3 ] && echo true || echo false)"

section "E2E A: delete echo-pipeline"
RESP=$(curl -sf -X DELETE "$API/workflows/$WF_ID")
check "deleted id" "$WF_ID" "$(jq_val "$RESP" '.id')"
check "deleted status" "Deleted" "$(jq_val "$RESP" '.status')"
CLEANUP_IDS=""

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API/workflows/$WF_ID")
check "404 for deleted workflow" "404" "$HTTP_CODE"

section "E2E B: submit echo-service-pipeline"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d @"$ECHO_SVC_PIPELINE")
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')

if [ "$HTTP_CODE" != "201" ]; then
  echo "  FAIL: could not create echo-service-pipeline (HTTP $HTTP_CODE)"
  echo "  Response: $BODY"
  FAILED=$((FAILED + 1))
  exit 1
fi

SVC_WF_ID=$(echo "$BODY" | jq -r '.id')
CLEANUP_IDS="$SVC_WF_ID"

check "echo-service-pipeline created (201)" "201" "$HTTP_CODE"
check "echo-service-pipeline status is Running" "Running" "$(echo "$BODY" | jq -r '.status')"
check "has volume" "1" "$(echo "$BODY" | jq -r '.volumes | length')"
check "has service" "1" "$(echo "$BODY" | jq -r '[.nodes[] | select(.type=="service")] | length')"
check "has 2 tiers" "2" "$(echo "$BODY" | jq -r '.dag.tiers | length')"

section "E2E B: wait for echo-service-pipeline to complete"
wait_for "echo-service-pipeline Succeeded" "curl -sf '$API/workflows/$SVC_WF_ID' | jq -r '.status'" "Succeeded" 180

section "E2E B: verify final state"
RESP=$(curl -sf "$API/workflows/$SVC_WF_ID")
check "workflow status is Succeeded" "Succeeded" "$(echo "$RESP" | jq -r '.status')"

WRITE_TASK=$(echo "$RESP" | jq -r '.nodes[] | select(.name=="write-data") | .status')
check "write-data Succeeded" "Succeeded" "$WRITE_TASK"

READ_TASK=$(echo "$RESP" | jq -r '.nodes[] | select(.name=="read-data") | .status')
check "read-data Succeeded" "Succeeded" "$READ_TASK"

ECHO_SVC=$(echo "$RESP" | jq -r '.nodes[] | select(.name=="echo-svc") | .status')
check "echo-svc was Running or Stopped" "true" "$([ "$ECHO_SVC" = "Running" ] || [ "$ECHO_SVC" = "Stopped" ] || [ "$ECHO_SVC" = "Succeeded" ] && echo true || echo false)"

section "E2E B: delete echo-service-pipeline"
RESP=$(curl -sf -X DELETE "$API/workflows/$SVC_WF_ID")
check "deleted id" "$SVC_WF_ID" "$(jq_val "$RESP" '.id')"
CLEANUP_IDS=""

echo ""
if [ "$FAILED" -eq 0 ]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit $FAILED
