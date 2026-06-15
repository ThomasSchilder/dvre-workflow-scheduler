#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_scheduler

section "Visualize: minimal workflow returns SVG"
RESP=$(curl -sf -X POST "$API/visualize" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "viz-minimal" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "main": {
        "binding": "local",
        "tasks": { "hello": { "image": "alpine:latest", "command": ["echo", "hello"] } }
      }
    }
  }')
CT=$(curl -sf -X POST "$API/visualize" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "viz-minimal" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "main": {
        "binding": "local",
        "tasks": { "hello": { "image": "alpine:latest", "command": ["echo", "hello"] } }
      }
    }
  }' -w '%{content_type}' -o /dev/null -s)
check "content type is svg+xml" "true" "$(echo "$CT" | grep -q 'image/svg+xml' && echo true || echo false)"
check "response contains svg tag" "true" "$(echo "$RESP" | grep -q '<svg' && echo true || echo false)"
check "response contains node label" "true" "$(echo "$RESP" | grep -q 'hello' && echo true || echo false)"

section "Visualize: invalid workflow returns 400"
HTTP=$(curl -sf -X POST "$API/visualize" \
  -H "Content-Type: application/json" \
  -d '{}' -w '%{http_code}' -o /dev/null -s || true)
check "400 for invalid workflow" "400" "$HTTP"

section "Visualize: workflow with volumes shows mount on node label"
RESP=$(curl -sf -X POST "$API/visualize" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "viz-volumes" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "volumes": {
      "data": { "size": "5Gi", "accessMode": "ReadWriteOnce" }
    },
    "sections": {
      "setup": {
        "binding": "local",
        "volumes": ["data"],
        "tasks": { "init": { "image": "alpine:latest", "command": ["sh", "-c", "init"], "volumeMounts": { "data": "/data" } } }
      }
    }
  }')
check "volume mount on node label" "true" "$(echo "$RESP" | grep -q 'data:/data' && echo true || echo false)"

section "Visualize: workflow with externalRefs shows ref on node label"
RESP=$(curl -sf -X POST "$API/visualize" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "viz-refs" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "externalRefs": {
      "model-data": { "source": "asset", "assetId": 42 }
    },
    "sections": {
      "train": {
        "binding": "local",
        "tasks": { "train-model": { "image": "alpine:latest", "command": ["train"], "externalRefs": ["model-data"] } }
      }
    }
  }')
check "externalRef on node label" "true" "$(echo "$RESP" | grep -q 'externalRef:model' && echo true || echo false)"

section "Visualize: section-level dependsOn shows cluster edges"
RESP=$(curl -sf -X POST "$API/visualize" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "viz-deps" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "ingest": {
        "binding": "local",
        "tasks": { "fetch": { "image": "alpine:latest", "command": ["fetch"] } }
      },
      "process": {
        "binding": "local",
        "dependsOn": ["ingest"],
        "tasks": { "transform": { "image": "alpine:latest", "command": ["transform"] } }
      }
    }
  }')
check "has cluster edge" "true" "$(echo "$RESP" | grep -q 'cluster_ingest' && echo true || echo false)"
check "has section label with infra" "true" "$(echo "$RESP" | grep -q 'Ingest' && echo true || echo false)"

section "Visualize: service with no dependents gets dashed ellipse"
RESP=$(curl -sf -X POST "$API/visualize" \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "v1",
    "metadata": { "name": "viz-service" },
    "infrastructure": {
      "local": { "source": "direct", "type": "kubernetes", "endpoint": "http://localhost:8080" }
    },
    "sections": {
      "serve": {
        "binding": "local",
        "tasks": { "report": { "image": "alpine:latest", "command": ["report"] } },
        "services": { "dashboard": { "image": "nginx", "port": 80 } }
      }
    }
  }')
check "service is ellipse" "true" "$(echo "$RESP" | grep -q '<ellipse' && echo true || echo false)"
check "service is dashed" "true" "$(echo "$RESP" | grep -q 'stroke-dasharray' && echo true || echo false)"

echo ""
if [ $FAILED -eq 0 ]; then
  echo "ALL TESTS PASSED"
else
  echo "SOME TESTS FAILED ($FAILED)"
  exit 1
fi
