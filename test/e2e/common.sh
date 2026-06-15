#!/usr/bin/env bash
set -euo pipefail

API="${SCHEDULER_API:-http://localhost:3000/api/v1}"
PASSED=0
FAILED=0

jq_val() {
  echo "$1" | jq -r "$2"
}

wf_field() {
  echo "$1" | jq -r "if .id then $2 else .workflow$2 end"
}

check() {
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $label"
    PASSED=$((PASSED + 1))
  else
    echo "  FAIL: $label (expected=$expected actual=$actual)"
    FAILED=$((FAILED + 1))
  fi
}

section() {
  echo ""
  echo "=== $1 ==="
}

wait_for() {
  local label="$1" cmd="$2" expected="$3" timeout="${4:-30}"
  local elapsed=0
  while [ $elapsed -lt $timeout ]; do
    local result
    result=$(eval "$cmd" 2>/dev/null || echo "")
    if [ "$result" = "$expected" ]; then
      echo "  PASS: $label"
      PASSED=$((PASSED + 1))
      return
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  echo "  FAIL: $label (timeout after ${timeout}s, last=$result)"
  FAILED=$((FAILED + 1))
}

require_scheduler() {
  curl -sf "$API/health" >/dev/null 2>&1 || {
    echo "ERROR: Scheduler not reachable at $API"
    echo "Start it with: node src/index.js"
    exit 1
  }
}
