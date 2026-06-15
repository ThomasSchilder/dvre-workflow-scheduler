#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_scheduler

WF_ID=""
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

OPERATOR_URL="${OPERATOR_URL:-http://localhost:8080}"

section "Tracker: create 3-tier sequential workflow"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "tracker-test" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "main": {
        "binding": "local",
        "executionMode": "sequential",
        "tasks": {
          "step1": { "image": "alpine:3.18", "command": ["echo", "1"] },
          "step2": { "image": "alpine:3.18", "command": ["echo", "2"] },
          "step3": { "image": "alpine:3.18", "command": ["echo", "3"] }
        }
      }
    }
  }')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')

if [ "$HTTP_CODE" != "201" ]; then
  echo "  FAIL: could not create workflow (HTTP $HTTP_CODE)"
  echo "  Response: $BODY"
  FAILED=$((FAILED + 1))
  exit 1
fi

WF_ID=$(echo "$BODY" | jq -r '.id')
CLEANUP_IDS="$WF_ID"

check "workflow created (201)" "201" "$HTTP_CODE"
check "workflow status is Running" "Running" "$(echo "$BODY" | jq -r '.status')"
check "3 nodes" "3" "$(echo "$BODY" | jq -r '.nodes | length')"

section "Tracker: only tier-0 task deployed at operator"
OPERATOR_TASKS=$(curl -sf "$OPERATOR_URL/api/v1/workflows/$WF_ID/tasks" 2>/dev/null || echo '{"data":[]}')
TASK_COUNT=$(echo "$OPERATOR_TASKS" | jq -r '.data | length' 2>/dev/null || echo "0")
check "1 task on operator (tier-0 only)" "1" "$TASK_COUNT"

section "Tracker: send task.succeeded for tier-0 — triggers tier advancement"
TIER0_TASK_ID=$(echo "$BODY" | jq -r '.nodes[] | select(.tier==0 and .type=="task") | .operatorResourceId')
if [ -z "$TIER0_TASK_ID" ] || [ "$TIER0_TASK_ID" = "null" ]; then
  echo "  FAIL: no tier-0 task operatorResourceId"
  FAILED=$((FAILED + 1))
else
  curl -sf -X POST "$API/webhooks/operator" \
    -H "Content-Type: application/json" \
    -d '{
      "event": "task.succeeded",
      "workflowId": "'"$WF_ID"'",
      "resourceId": "'"$TIER0_TASK_ID"'",
      "resourceType": "task",
      "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"'",
      "details": { "phase": "Succeeded" }
    }' >/dev/null

  sleep 1

  RESP=$(curl -sf "$API/workflows/$WF_ID")
  TIER0_STATUS=$(echo "$RESP" | jq -r '.nodes[] | select(.tier==0 and .type=="task") | .status')
  check "tier-0 task status is Succeeded" "Succeeded" "$TIER0_STATUS"

  TIER1_TASK_ID=$(echo "$RESP" | jq -r '.nodes[] | select(.tier==1 and .type=="task") | .operatorResourceId')
  check "tier-1 task deployed (operatorResourceId)" "true" "$([ "$TIER1_TASK_ID" != "null" ] && [ -n "$TIER1_TASK_ID" ] && echo true || echo false)"

  OPERATOR_TASKS=$(curl -sf "$OPERATOR_URL/api/v1/workflows/$WF_ID/tasks" 2>/dev/null || echo '{"data":[]}')
  TASK_COUNT=$(echo "$OPERATOR_TASKS" | jq -r '.data | length' 2>/dev/null || echo "0")
  check "2 tasks on operator after tier advancement" "2" "$TASK_COUNT"
fi

section "Tracker: send task.succeeded for tier-1 — advances to tier-2"
if [ -n "$TIER1_TASK_ID" ] && [ "$TIER1_TASK_ID" != "null" ]; then
  curl -sf -X POST "$API/webhooks/operator" \
    -H "Content-Type: application/json" \
    -d '{
      "event": "task.succeeded",
      "workflowId": "'"$WF_ID"'",
      "resourceId": "'"$TIER1_TASK_ID"'",
      "resourceType": "task",
      "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"'",
      "details": { "phase": "Succeeded" }
    }' >/dev/null

  sleep 1

  RESP=$(curl -sf "$API/workflows/$WF_ID")
  TIER2_TASK_ID=$(echo "$RESP" | jq -r '.nodes[] | select(.tier==2 and .type=="task") | .operatorResourceId')
  check "tier-2 task deployed (operatorResourceId)" "true" "$([ "$TIER2_TASK_ID" != "null" ] && [ -n "$TIER2_TASK_ID" ] && echo true || echo false)"
fi

section "Tracker: send task.succeeded for tier-2 — workflow Succeeded"
if [ -n "$TIER2_TASK_ID" ] && [ "$TIER2_TASK_ID" != "null" ]; then
  curl -sf -X POST "$API/webhooks/operator" \
    -H "Content-Type: application/json" \
    -d '{
      "event": "task.succeeded",
      "workflowId": "'"$WF_ID"'",
      "resourceId": "'"$TIER2_TASK_ID"'",
      "resourceType": "task",
      "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"'",
      "details": { "phase": "Succeeded" }
    }' >/dev/null

  sleep 1

  RESP=$(curl -sf "$API/workflows/$WF_ID")
  check "workflow status is Succeeded" "Succeeded" "$(echo "$RESP" | jq -r '.status')"
fi

section "Tracker: task.failed → workflow Failed"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "failure-test" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "main": {
        "binding": "local",
        "tasks": {
          "hello": { "image": "alpine:3.18", "command": ["echo", "hello"] }
        }
      }
    }
  }')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')

if [ "$HTTP_CODE" = "201" ]; then
  FAIL_WF_ID=$(echo "$BODY" | jq -r '.id')
  CLEANUP_IDS="$FAIL_WF_ID"

  FAIL_TASK_ID=$(echo "$BODY" | jq -r '.nodes[] | select(.tier==0 and .type=="task") | .operatorResourceId')

  curl -sf -X POST "$API/webhooks/operator" \
    -H "Content-Type: application/json" \
    -d '{
      "event": "task.failed",
      "workflowId": "'"$FAIL_WF_ID"'",
      "resourceId": "'"$FAIL_TASK_ID"'",
      "resourceType": "task",
      "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"'",
      "details": { "phase": "Failed" }
    }' >/dev/null

  sleep 1

  RESP=$(curl -sf "$API/workflows/$FAIL_WF_ID")
  check "workflow status is Failed" "Failed" "$(echo "$RESP" | jq -r '.status')"

  CLEANUP_IDS=""
else
  echo "  SKIP: failure test (operator down, HTTP $HTTP_CODE)"
fi

echo ""
if [ "$FAILED" -eq 0 ]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit $FAILED
