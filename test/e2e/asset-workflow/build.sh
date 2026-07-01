#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REGISTRY="thomasschilder"

build_and_push() {
  local name="$1"
  local dir="$2"
  local tag="${REGISTRY}/${name}:latest"

  echo "Building ${name}..."
  docker build -t "${name}" "${dir}"
  docker tag "${name}" "${tag}"
  echo "Pushing ${tag}..."
  docker push "${tag}"
  echo "Done: ${tag}"
}

build_and_push "dvre-test-workflow-task-hello-world" "${SCRIPT_DIR}/hello-world"
build_and_push "dvre-test-workflow-task-reverse" "${SCRIPT_DIR}/reverse"

echo ""
echo "All images built and pushed:"
echo "  ${REGISTRY}/dvre-test-workflow-task-hello-world:latest"
echo "  ${REGISTRY}/dvre-test-workflow-task-reverse:latest"
