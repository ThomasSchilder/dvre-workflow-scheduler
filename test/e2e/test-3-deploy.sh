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

section "Deploy: create workflow (operator may be up or down)"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "deploy-test" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "ingestion": {
        "binding": "local",
        "tasks": {
          "fetch-data": { "image": "alpine:3.18", "command": ["echo", "fetching"] }
        },
        "services": {
          "api-service": { "image": "node:20", "port": 3000 }
        }
      },
      "processing": {
        "binding": "local",
        "dependsOn": ["ingestion"],
        "tasks": {
          "transform": { "image": "python:3.11", "command": ["python", "transform.py"] }
        }
      }
    }
  }')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')

WF_ID=$(echo "$BODY" | jq -r '.id // .workflow.id // empty' 2>/dev/null)
if [ -n "$WF_ID" ]; then
  CLEANUP_IDS="$WF_ID"
fi

if [ "$HTTP_CODE" = "201" ]; then
  echo "  PASS: deploy succeeded (201) — operator is up"
  PASSED=$((PASSED + 1))

  check "workflow status is Running" "Running" "$(echo "$BODY" | jq -r '.status')"
  check "workflow has nodes" "3" "$(echo "$BODY" | jq -r '.nodes | length')"
  check "workflow has infra" "true" "$(echo "$BODY" | jq -r '.infra != null')"

  TIER0_TASKS=$(echo "$BODY" | jq -r '[.nodes[] | select(.tier==0 and .type=="task")] | length')
  check "tier-0 task has operatorResourceId" "true" "$(echo "$BODY" | jq -r '[.nodes[] | select(.tier==0 and .type=="task" and .operatorResourceId != null)] | length == '"$TIER0_TASKS"' | tostring')"

  TIER0_SERVICES=$(echo "$BODY" | jq -r '[.nodes[] | select(.tier==0 and .type=="service")] | length')
  if [ "$TIER0_SERVICES" -gt 0 ]; then
    check "tier-0 service has operatorResourceId" "true" "$(echo "$BODY" | jq -r '[.nodes[] | select(.tier==0 and .type=="service" and .operatorResourceId != null)] | length == '"$TIER0_SERVICES"' | tostring')"
  fi

  TIER1_NODES=$(echo "$BODY" | jq -r '[.nodes[] | select(.tier==1)] | length')
  if [ "$TIER1_NODES" -gt 0 ]; then
    check "tier-1 node NOT deployed yet" "true" "$(echo "$BODY" | jq -r '[.nodes[] | select(.tier==1 and .operatorResourceId == null)] | length == '"$TIER1_NODES"' | tostring')"
  fi

  section "Deploy: GET workflow includes volumes"
  RESP=$(curl -sf "$API/workflows/$WF_ID")
  check "volumes array present" "true" "$(jq_val "$RESP" '.volumes != null')"

  section "Deploy: operator has workflow"
  OPERATOR_RESP=$(curl -sf "$OPERATOR_URL/api/v1/workflows/$WF_ID" 2>/dev/null || echo '{}')
  if echo "$OPERATOR_RESP" | jq -e '.id' >/dev/null 2>&1; then
    echo "  PASS: workflow exists on operator"
    PASSED=$((PASSED + 1))
  else
    echo "  SKIP: could not verify workflow on operator (operator may not be reachable)"
  fi

  section "Deploy: operator has tier-0 task"
  OPERATOR_TASKS=$(curl -sf "$OPERATOR_URL/api/v1/workflows/$WF_ID/tasks" 2>/dev/null || echo '{"data":[]}')
  TASK_COUNT=$(echo "$OPERATOR_TASKS" | jq -r '.data | length' 2>/dev/null || echo "0")
  if [ "$TASK_COUNT" -ge 1 ]; then
    echo "  PASS: $TASK_COUNT task(s) on operator"
    PASSED=$((PASSED + 1))
  else
    echo "  SKIP: could not verify tasks on operator"
  fi

  section "Deploy: operator has tier-0 service"
  OPERATOR_SERVICES=$(curl -sf "$OPERATOR_URL/api/v1/workflows/$WF_ID/services" 2>/dev/null || echo '{"data":[]}')
  SERVICE_COUNT=$(echo "$OPERATOR_SERVICES" | jq -r '.data | length' 2>/dev/null || echo "0")
  if [ "$SERVICE_COUNT" -ge 1 ]; then
    echo "  PASS: $SERVICE_COUNT service(s) on operator"
    PASSED=$((PASSED + 1))
  else
    echo "  SKIP: could not verify services on operator"
  fi

  section "Deploy: delete deployed workflow"
  RESP=$(curl -sf -X DELETE "$API/workflows/$WF_ID")
  check "deleted id" "$WF_ID" "$(jq_val "$RESP" '.id')"
  check "deleted status" "Deleted" "$(jq_val "$RESP" '.status')"
  CLEANUP_IDS=""

  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API/workflows/$WF_ID")
  check "404 for deleted workflow" "404" "$HTTP_CODE"

elif [ "$HTTP_CODE" = "502" ]; then
  echo "  PASS: deploy failed (502) — operator is down, as expected"
  PASSED=$((PASSED + 1))

  WF_STATUS=$(echo "$BODY" | jq -r '.workflow.status // .status // empty' 2>/dev/null)
  check "workflow status is Failed" "Failed" "$WF_STATUS"
  check "deploy error present" "true" "$(echo "$BODY" | jq -r '(.workflow.deployError // .deployError // empty) != null' 2>/dev/null || echo false)"

  check "workflow has nodes" "3" "$(echo "$BODY" | jq -r '(.workflow.nodes // .nodes) | length' 2>/dev/null)"
  check "workflow has dag" "true" "$(echo "$BODY" | jq -r '(.workflow.dag // .dag) != null' 2>/dev/null)"

  section "Deploy: delete failed workflow"
  if [ -n "$WF_ID" ]; then
    RESP=$(curl -sf -X DELETE "$API/workflows/$WF_ID")
    check "deleted id" "$WF_ID" "$(jq_val "$RESP" '.id')"
    check "deleted status" "Deleted" "$(jq_val "$RESP" '.status')"
    CLEANUP_IDS=""

    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API/workflows/$WF_ID")
    check "404 for deleted workflow" "404" "$HTTP_CODE"
  fi
else
  echo "  FAIL: unexpected HTTP status $HTTP_CODE"
  FAILED=$((FAILED + 1))
fi

section "Deploy: validation failure returns 400"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{"invalid": true}')
check "400 for invalid workflow" "400" "$HTTP_CODE"

section "Deploy: DAG cycle returns 400"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "cycle-test" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "a": { "tasks": { "x": { "image": "alpine", "dependsOn": ["b.y"] } } },
      "b": { "tasks": { "y": { "image": "alpine", "dependsOn": ["a.x"] } } }
    }
  }')
check "400 for DAG cycle" "400" "$HTTP_CODE"

section "Deploy: 404 on non-existent workflow"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API/workflows/wf-nonexistent")
check "404 for non-existent workflow" "404" "$HTTP_CODE"

section "Deploy: workflow with volumes"
RESP=$(curl -s -w "\n%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "vol-test" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "volumes": {
      "shared-data": { "size": "10Gi", "storageClass": "ssd", "accessMode": "ReadWriteMany" }
    },
    "sections": {
      "main": {
        "binding": "local",
        "volumes": ["shared-data"],
        "tasks": {
          "process": { "image": "alpine", "command": ["sh"], "volumeMounts": { "shared-data": "/mnt/data" } }
        }
      }
    }
  }')
HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
VOL_WF_ID=$(echo "$BODY" | jq -r '.id // .workflow.id // empty' 2>/dev/null)
if [ -n "$VOL_WF_ID" ]; then
  CLEANUP_IDS="$CLEANUP_IDS $VOL_WF_ID"
fi

if [ "$HTTP_CODE" = "201" ]; then
  check "volume workflow status is Running" "Running" "$(echo "$BODY" | jq -r '.status')"
  VOL_COUNT=$(echo "$BODY" | jq -r '.volumes | length' 2>/dev/null || echo "0")
  check "volume in response" "true" "$([ "$VOL_COUNT" -ge 1 ] && echo true || echo false)"

  RESP=$(curl -sf "$API/workflows/$VOL_WF_ID")
  check "GET includes volumes" "true" "$(jq_val "$RESP" '.volumes != null')"
  check "volume name is shared-data" "shared-data" "$(jq_val "$RESP" '.volumes[0].name')"
  check "volume size is 10Gi" "10Gi" "$(jq_val "$RESP" '.volumes[0].size')"
elif [ "$HTTP_CODE" = "502" ]; then
  echo "  PASS: volume workflow failed (operator down)"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: unexpected HTTP status $HTTP_CODE for volume workflow"
  FAILED=$((FAILED + 1))
fi

section "Deploy: webhook with operator resource ID"
if [ -n "${VOL_WF_ID:-}" ]; then
  RESP=$(curl -sf -X POST "$API/webhooks/operator" \
    -H "Content-Type: application/json" \
    -d '{
      "event": "task.succeeded",
      "workflowId": "'"$VOL_WF_ID"'",
      "resourceId": "'"$VOL_WF_ID"'-main-process",
      "resourceType": "task",
      "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"'",
      "details": { "phase": "Succeeded" }
    }')
  check "webhook accepted" "true" "$(jq_val "$RESP" '.ok')"

  RESP=$(curl -sf "$API/workflows/$VOL_WF_ID")
  check "node status updated by webhook" "Succeeded" "$(jq_val "$RESP" '.nodes[0].status')"
else
  echo "  SKIP: webhook test (no deployed workflow)"
fi

echo ""
if [ "$FAILED" -eq 0 ]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit $FAILED
