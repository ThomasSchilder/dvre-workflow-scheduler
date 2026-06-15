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

section "DAG: minimal workflow — 1 task, tier 0"
RESP=$(curl -s -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "dag-minimal" },
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
CLEANUP_IDS="$WF_ID"

WF_STATUS=$(wf_field "$RESP" '.status')
if [ "$WF_STATUS" = "Running" ] || [ "$WF_STATUS" = "Failed" ]; then
  echo "  PASS: workflow status is $WF_STATUS (deploy attempted)"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: workflow status is $WF_STATUS (expected Running or Failed)"
  FAILED=$((FAILED + 1))
fi
check "1 node" "1" "$(wf_field "$RESP" '.nodes | length')"
check "node id" "${WF_ID}.main.hello" "$(wf_field "$RESP" '.nodes[0].id')"
check "node type" "task" "$(wf_field "$RESP" '.nodes[0].type')"
check "node tier" "0" "$(wf_field "$RESP" '.nodes[0].tier')"
check "node section" "main" "$(wf_field "$RESP" '.nodes[0].section')"
check "node name" "hello" "$(wf_field "$RESP" '.nodes[0].name')"
check "node status" "Pending" "$(wf_field "$RESP" '.nodes[0].status')"
check "no dependsOn" "0" "$(wf_field "$RESP" '.nodes[0].dependsOn | length')"

check "dag has nodes" "1" "$(wf_field "$RESP" '.dag.nodes | length')"
check "dag has tiers" "1" "$(wf_field "$RESP" '.dag.tiers | length')"

section "DAG: two sections with section-level dependsOn"
RESP=$(curl -s -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "dag-sections" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "ingestion": {
        "tasks": {
          "fetch": { "image": "alpine:latest" }
        }
      },
      "processing": {
        "dependsOn": ["ingestion"],
        "tasks": {
          "validate": { "image": "python:3.11" }
        }
      }
    }
  }')
WF2_ID=$(wf_field "$RESP" '.id')
CLEANUP_IDS="$CLEANUP_IDS $WF2_ID"

check "2 nodes" "2" "$(wf_field "$RESP" '.nodes | length')"
check "fetch tier 0" "0" "$(wf_field "$RESP" '.nodes[] | select(.name=="fetch") | .tier')"
check "validate tier 1" "1" "$(wf_field "$RESP" '.nodes[] | select(.name=="validate") | .tier')"
check "validate depends on fetch" "1" "$(wf_field "$RESP" '.nodes[] | select(.name=="validate") | .dependsOn | length')"
check "validate depends on fetch id" "ingestion.fetch" "$(wf_field "$RESP" '.nodes[] | select(.name=="validate") | .dependsOn[0]')"

section "DAG: sequential executionMode chains tasks"
RESP=$(curl -s -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "dag-sequential" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "pipeline": {
        "executionMode": "sequential",
        "tasks": {
          "step1": { "image": "alpine:latest" },
          "step2": { "image": "alpine:latest" },
          "step3": { "image": "alpine:latest" }
        }
      }
    }
  }')
WF3_ID=$(wf_field "$RESP" '.id')
CLEANUP_IDS="$CLEANUP_IDS $WF3_ID"

check "3 nodes" "3" "$(wf_field "$RESP" '.nodes | length')"
check "step1 tier 0" "0" "$(wf_field "$RESP" '.nodes[] | select(.name=="step1") | .tier')"
check "step2 tier 1" "1" "$(wf_field "$RESP" '.nodes[] | select(.name=="step2") | .tier')"
check "step3 tier 2" "2" "$(wf_field "$RESP" '.nodes[] | select(.name=="step3") | .tier')"
check "step2 depends on step1" "1" "$(wf_field "$RESP" '.nodes[] | select(.name=="step2") | .dependsOn | length')"
check "step3 depends on step2" "1" "$(wf_field "$RESP" '.nodes[] | select(.name=="step3") | .dependsOn | length')"

section "DAG: service with no task dependents gets desiredPhase Running"
RESP=$(curl -s -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "dag-service" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "main": {
        "tasks": {
          "hello": { "image": "alpine:latest" }
        },
        "services": {
          "web": { "image": "nginx:alpine", "port": 80 }
        }
      }
    }
  }')
WF4_ID=$(wf_field "$RESP" '.id')
CLEANUP_IDS="$CLEANUP_IDS $WF4_ID"

check "2 nodes" "2" "$(wf_field "$RESP" '.nodes | length')"
check "service desiredPhase" "Running" "$(wf_field "$RESP" '.nodes[] | select(.type=="service") | .desiredPhase')"
check "task desiredPhase null" "null" "$(wf_field "$RESP" '.nodes[] | select(.type=="task") | .desiredPhase')"

section "DAG: cycle detection returns 400"
HTTP_CODE=$(curl -s -o /tmp/dag-cycle-resp.json -w "%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "dag-cycle" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "a": {
        "tasks": { "x": { "image": "alpine", "dependsOn": ["b.y"] } }
      },
      "b": {
        "tasks": { "y": { "image": "alpine", "dependsOn": ["a.x"] } }
      }
    }
  }')
check "cycle returns 400" "400" "$HTTP_CODE"
CYCLE_DETAILS=$(cat /tmp/dag-cycle-resp.json | jq -r '.details')
if echo "$CYCLE_DETAILS" | grep -qi "cycle"; then
  echo "  PASS: details mention cycle"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL: details mention cycle (actual=$CYCLE_DETAILS)"
  FAILED=$((FAILED + 1))
fi
rm -f /tmp/dag-cycle-resp.json

section "DAG: invalid dependsOn returns 400"
HTTP_CODE=$(curl -s -o /tmp/dag-invalid-resp.json -w "%{http_code}" -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "dag-invalid" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "main": {
        "tasks": { "x": { "image": "alpine", "dependsOn": ["nonexistent"] } }
      }
    }
  }')
check "invalid dep returns 400" "400" "$HTTP_CODE"
check "error is DAG resolution failed" "DAG resolution failed" "$(jq_val "$(cat /tmp/dag-invalid-resp.json)" '.error')"
rm -f /tmp/dag-invalid-resp.json

section "DAG: GET /workflows/:id includes dag and nodes"
RESP=$(curl -sf "$API/workflows/$WF_ID")
check "dag present" "true" "$(jq_val "$RESP" '.dag != null')"
check "dag has main.hello" "task" "$(jq_val "$RESP" '.dag.nodes["main.hello"].type')"
check "nodes present" "1" "$(jq_val "$RESP" '.nodes | length')"

section "DAG: binding inheritance"
RESP=$(curl -s -X POST "$API/workflows" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "dag-binding" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" },
      "gpu": { "source": "direct", "type": "kubernetes", "endpoint": "http://gpu-cluster:8080" }
    },
    "sections": {
      "main": {
        "binding": "local",
        "tasks": {
          "a": { "image": "alpine:latest" },
          "b": { "image": "alpine:latest", "binding": "gpu" }
        }
      }
    }
  }')
WF5_ID=$(wf_field "$RESP" '.id')
CLEANUP_IDS="$CLEANUP_IDS $WF5_ID"

check "a inherits section binding" "local" "$(wf_field "$RESP" '.nodes[] | select(.name=="a") | .infraBinding')"
check "b overrides section binding" "gpu" "$(wf_field "$RESP" '.nodes[] | select(.name=="b") | .infraBinding')"

echo ""
if [ "$FAILED" -eq 0 ]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED"
fi
exit $FAILED
