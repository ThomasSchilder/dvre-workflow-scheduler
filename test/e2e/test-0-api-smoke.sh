#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_scheduler

WF_ID=""
WF2_ID=""

cleanup() {
  echo ""
  echo "=== Cleanup ==="
  if [ -n "${WF_ID:-}" ]; then
    curl -sf -X DELETE "$API/workflows/$WF_ID" >/dev/null 2>&1 && echo "  Deleted workflow $WF_ID"
  fi
  if [ -n "${WF2_ID:-}" ]; then
    curl -sf -X DELETE "$API/workflows/$WF2_ID" >/dev/null 2>&1 && echo "  Deleted workflow $WF2_ID"
  fi
  rm -f /tmp/sse-events-test0.txt
  echo "  Cleanup done"
}

trap cleanup EXIT

section "Health check"
RESP=$(curl -sf "$API/health")
check "status is ok" "ok" "$(jq_val "$RESP" '.status')"
check "version present" "0.1.0" "$(jq_val "$RESP" '.version')"

section "Create workflow (minimal)"
RESP=$(curl -s -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "smoke-test" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "main": {
        "tasks": {
          "hello": { "image": "alpine:latest", "command": ["echo", "hello"] }
        }
      }
    }
  }')
WF_ID=$(wf_field "$RESP" '.id')
check "workflow id starts with wf-" "wf-" "$(echo "$WF_ID" | cut -c1-3)"
check "workflow name" "smoke-test" "$(wf_field "$RESP" '.name')"
WF_STATUS=$(wf_field "$RESP" '.status')
if [ "$WF_STATUS" = "Running" ] || [ "$WF_STATUS" = "Failed" ]; then
  echo "  PASS: workflow status is $WF_STATUS (deploy attempted)"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: workflow status is $WF_STATUS (expected Running or Failed)"
  FAILED=$((FAILED + 1))
fi
check "workflow config preserved" "smoke-test" "$(wf_field "$RESP" '.config.metadata.name')"

section "Get workflow"
RESP=$(curl -sf "$API/workflows/$WF_ID")
check "workflow id" "$WF_ID" "$(jq_val "$RESP" '.id')"
check "workflow name" "smoke-test" "$(jq_val "$RESP" '.name')"
check "nodes array present" "1" "$(jq_val "$RESP" '.nodes | length')"

section "List workflows"
RESP=$(curl -sf "$API/workflows")
WF_COUNT=$(jq_val "$RESP" '.data | length')
if [ "$WF_COUNT" -ge 1 ]; then
  echo "  PASS: at least 1 workflow ($WF_COUNT)"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: no workflows listed"
  FAILED=$((FAILED + 1))
fi

section "Create second workflow"
RESP=$(curl -s -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "second-test" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "main": {
        "tasks": {
          "world": { "image": "alpine:latest", "command": ["echo", "world"] }
        }
      }
    }
  }')
WF2_ID=$(wf_field "$RESP" '.id')
check "second workflow id starts with wf-" "wf-" "$(echo "$WF2_ID" | cut -c1-3)"

RESP=$(curl -sf "$API/workflows")
WF_COUNT=$(jq_val "$RESP" '.data | length')
if [ "$WF_COUNT" -ge 2 ]; then
  echo "  PASS: at least 2 workflows ($WF_COUNT)"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: expected 2+ workflows, got $WF_COUNT"
  FAILED=$((FAILED + 1))
fi

section "Delete workflow"
RESP=$(curl -sf -X DELETE "$API/workflows/$WF2_ID")
check "deleted id" "$WF2_ID" "$(jq_val "$RESP" '.id')"
check "deleted status" "Deleted" "$(jq_val "$RESP" '.status')"

RESP=$(curl -sf "$API/workflows")
WF_COUNT=$(jq_val "$RESP" '.data | length')
if [ "$WF_COUNT" -eq 1 ]; then
  echo "  PASS: 1 workflow remaining after delete"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: expected 1 workflow, got $WF_COUNT"
  FAILED=$((FAILED + 1))
fi

section "Get deleted workflow returns 404"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API/workflows/$WF2_ID")
check "404 for deleted workflow" "404" "$HTTP_CODE"

section "Validation: invalid workflow returns 400"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{"invalid": true}')
check "400 for invalid workflow" "400" "$HTTP_CODE"

section "SSE endpoint — connect first, then send webhook"
SSE_FILE="/tmp/sse-events-test0.txt"
rm -f "$SSE_FILE"
timeout 8 curl -sN "$API/workflows/$WF_ID/events" > "$SSE_FILE" 2>/dev/null &
SSE_PID=$!
sleep 1

if grep -q "connected" "$SSE_FILE" 2>/dev/null; then
  echo "  PASS: SSE connected event received"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: SSE connected event not received"
  FAILED=$((FAILED + 1))
fi

section "Webhook callback (while SSE connected)"
OPERATOR_TASK_ID="${WF_ID}-main-hello"
RESP=$(curl -sf -X POST "$API/webhooks/operator" \
  -H "Content-Type: application/json" \
  -d "{
    \"event\": \"task.succeeded\",
    \"workflowId\": \"$WF_ID\",
    \"resourceId\": \"$OPERATOR_TASK_ID\",
    \"resourceType\": \"task\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",
    \"details\": { \"phase\": \"Succeeded\" }
  }")
check "webhook accepted" "true" "$(jq_val "$RESP" '.ok')"

sleep 2
kill "$SSE_PID" 2>/dev/null || true
wait "$SSE_PID" 2>/dev/null || true

if grep -q "task.succeeded" "$SSE_FILE" 2>/dev/null; then
  echo "  PASS: SSE task.succeeded event received"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: SSE task.succeeded event not received"
  FAILED=$((FAILED + 1))
fi

section "404 on non-existent workflow"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API/workflows/wf-nonexistent")
check "404 for non-existent workflow" "404" "$HTTP_CODE"

echo ""
if [ "$FAILED" -eq 0 ]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit $FAILED
