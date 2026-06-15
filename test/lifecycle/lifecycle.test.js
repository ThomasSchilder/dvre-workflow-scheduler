import {
  isTierComplete,
  getHighestCompletedTier,
  hasTaskFailed,
  isWorkflowComplete,
  getServicesToStart,
  getServicesToStop,
} from "../../src/lifecycle.js";

let passed = 0;
let failed = 0;

function assert(condition, label) {
  if (condition) {
    console.log(`  PASS: ${label}`);
    passed++;
  } else {
    console.log(`  FAIL: ${label}`);
    failed++;
  }
}

function makeNode(section, name, type, tier, status, desiredPhase) {
  return {
    section,
    name,
    type,
    tier,
    status,
    desired_phase: desiredPhase || null,
    operator_resource_id: type === "service" && desiredPhase ? `wf-test-${section}-${name}` : null,
  };
}

const tiers3 = [
  { tier: 0, nodes: ["main.step1"] },
  { tier: 1, nodes: ["main.step2"] },
  { tier: 2, nodes: ["main.step3"] },
];

const tiersParallel = [
  { tier: 0, nodes: ["main.fetch-a", "main.fetch-b"] },
  { tier: 1, nodes: ["main.process"] },
];

console.log("=== isTierComplete ===");

{
  const nodes = [
    makeNode("main", "step1", "task", 0, "Succeeded"),
    makeNode("main", "step2", "task", 1, "Pending"),
    makeNode("main", "step3", "task", 2, "Pending"),
  ];
  assert(isTierComplete(0, nodes, tiers3) === true, "tier 0 complete (all tasks Succeeded)");
  assert(isTierComplete(1, nodes, tiers3) === false, "tier 1 not complete (step2 Pending)");
  assert(isTierComplete(2, nodes, tiers3) === false, "tier 2 not complete (step3 Pending)");
}

{
  const nodes = [
    makeNode("main", "fetch-a", "task", 0, "Succeeded"),
    makeNode("main", "fetch-b", "task", 0, "Running"),
    makeNode("main", "process", "task", 1, "Pending"),
  ];
  assert(isTierComplete(0, nodes, tiersParallel) === false, "tier 0 not complete (one Running)");
}

{
  const nodes = [
    makeNode("main", "fetch-a", "task", 0, "Succeeded"),
    makeNode("main", "fetch-b", "task", 0, "Succeeded"),
    makeNode("main", "process", "task", 1, "Pending"),
  ];
  assert(isTierComplete(0, nodes, tiersParallel) === true, "tier 0 complete (both Succeeded)");
}

{
  const tierWithService = [
    { tier: 0, nodes: ["main.api-service", "main.hello"] },
  ];
  const nodes = [
    makeNode("main", "api-service", "service", 0, "Running"),
    makeNode("main", "hello", "task", 0, "Succeeded"),
  ];
  assert(isTierComplete(0, nodes, tierWithService) === true, "tier with only services + Succeeded task is complete");
}

{
  const tierOnlyServices = [
    { tier: 0, nodes: ["main.dashboard"] },
  ];
  const nodes = [
    makeNode("main", "dashboard", "service", 0, "Running"),
  ];
  assert(isTierComplete(0, nodes, tierOnlyServices) === true, "tier with only services (no tasks) is vacuously complete");
}

console.log("\n=== getHighestCompletedTier ===");

{
  const nodes = [
    makeNode("main", "step1", "task", 0, "Succeeded"),
    makeNode("main", "step2", "task", 1, "Succeeded"),
    makeNode("main", "step3", "task", 2, "Pending"),
  ];
  assert(getHighestCompletedTier(nodes, tiers3) === 1, "tiers 0 and 1 complete → returns 1");
}

{
  const nodes = [
    makeNode("main", "step1", "task", 0, "Succeeded"),
    makeNode("main", "step2", "task", 1, "Succeeded"),
    makeNode("main", "step3", "task", 2, "Succeeded"),
  ];
  assert(getHighestCompletedTier(nodes, tiers3) === 2, "all tiers complete → returns 2");
}

{
  const nodes = [
    makeNode("main", "step1", "task", 0, "Running"),
    makeNode("main", "step2", "task", 1, "Pending"),
    makeNode("main", "step3", "task", 2, "Pending"),
  ];
  assert(getHighestCompletedTier(nodes, tiers3) === -1, "no tier complete → returns -1");
}

{
  const nodes = [
    makeNode("main", "fetch-a", "task", 0, "Succeeded"),
    makeNode("main", "fetch-b", "task", 0, "Succeeded"),
    makeNode("main", "process", "task", 1, "Pending"),
  ];
  assert(getHighestCompletedTier(nodes, tiersParallel) === 0, "parallel tier 0 complete → returns 0");
}

console.log("\n=== hasTaskFailed ===");

{
  const nodes = [
    makeNode("main", "step1", "task", 0, "Failed"),
    makeNode("main", "step2", "task", 1, "Pending"),
  ];
  assert(hasTaskFailed(nodes) === true, "one task Failed → true");
}

{
  const nodes = [
    makeNode("main", "step1", "task", 0, "Succeeded"),
    makeNode("main", "step2", "task", 1, "Succeeded"),
  ];
  assert(hasTaskFailed(nodes) === false, "all tasks Succeeded → false");
}

{
  const nodes = [
    makeNode("main", "api", "service", 0, "Failed"),
    makeNode("main", "hello", "task", 0, "Succeeded"),
  ];
  assert(hasTaskFailed(nodes) === false, "service Failed doesn't count → false");
}

console.log("\n=== isWorkflowComplete ===");

{
  const nodes = [
    makeNode("main", "step1", "task", 0, "Succeeded"),
    makeNode("main", "step2", "task", 1, "Succeeded"),
    makeNode("main", "step3", "task", 2, "Succeeded"),
  ];
  assert(isWorkflowComplete(nodes, tiers3) === true, "all tasks Succeeded → true");
}

{
  const nodes = [
    makeNode("main", "step1", "task", 0, "Succeeded"),
    makeNode("main", "step2", "task", 1, "Running"),
    makeNode("main", "step3", "task", 2, "Pending"),
  ];
  assert(isWorkflowComplete(nodes, tiers3) === false, "one task Running → false");
}

{
  const nodes = [
    makeNode("main", "api", "service", 0, "Running"),
    makeNode("main", "hello", "task", 0, "Succeeded"),
  ];
  const tiers = [{ tier: 0, nodes: ["main.api", "main.hello"] }];
  assert(isWorkflowComplete(nodes, tiers) === true, "services ignored, all tasks Succeeded → true");
}

console.log("\n=== getServicesToStart ===");

{
  const serviceLifecycle = {
    "main.db-service": { startBeforeTier: 1, stopAfterTier: 2, noDependents: false },
    "main.cache": { startBeforeTier: 0, stopAfterTier: 1, noDependents: false },
    "main.dashboard": { startBeforeTier: null, stopAfterTier: null, noDependents: true },
  };
  const nodes = [
    makeNode("main", "db-service", "service", 0, "Pending", "Stopped"),
    makeNode("main", "cache", "service", 0, "Running", "Running"),
    makeNode("main", "dashboard", "service", 0, "Running", "Running"),
  ];
  const toStart = getServicesToStart(1, nodes, serviceLifecycle);
  assert(toStart.length === 1, "one service needs starting");
  assert(toStart[0].name === "db-service", "db-service starts before tier 1");
}

{
  const serviceLifecycle = {
    "main.db-service": { startBeforeTier: 1, stopAfterTier: 2, noDependents: false },
  };
  const nodes = [
    makeNode("main", "db-service", "service", 0, "Running", "Running"),
  ];
  const toStart = getServicesToStart(1, nodes, serviceLifecycle);
  assert(toStart.length === 0, "service already Running → not in start list");
}

{
  const serviceLifecycle = {
    "main.dashboard": { startBeforeTier: null, stopAfterTier: null, noDependents: true },
  };
  const nodes = [
    makeNode("main", "dashboard", "service", 0, "Running", "Running"),
  ];
  const toStart = getServicesToStart(1, nodes, serviceLifecycle);
  assert(toStart.length === 0, "noDependents service → never in start list");
}

console.log("\n=== getServicesToStop ===");

{
  const serviceLifecycle = {
    "main.db-service": { startBeforeTier: 0, stopAfterTier: 0, noDependents: false },
    "main.cache": { startBeforeTier: 0, stopAfterTier: 2, noDependents: false },
    "main.dashboard": { startBeforeTier: null, stopAfterTier: null, noDependents: true },
  };
  const nodes = [
    makeNode("main", "db-service", "service", 0, "Running", "Running"),
    makeNode("main", "cache", "service", 0, "Running", "Running"),
    makeNode("main", "dashboard", "service", 0, "Running", "Running"),
  ];
  const toStop = getServicesToStop(1, nodes, serviceLifecycle);
  assert(toStop.length === 1, "one service needs stopping");
  assert(toStop[0].name === "db-service", "db-service stopAfterTier=0 < nextTier=1");
}

{
  const serviceLifecycle = {
    "main.db-service": { startBeforeTier: 0, stopAfterTier: 0, noDependents: false },
  };
  const nodes = [
    makeNode("main", "db-service", "service", 0, "Stopped", "Stopped"),
  ];
  const toStop = getServicesToStop(1, nodes, serviceLifecycle);
  assert(toStop.length === 0, "service already Stopped → not in stop list");
}

{
  const serviceLifecycle = {
    "main.dashboard": { startBeforeTier: null, stopAfterTier: null, noDependents: true },
  };
  const nodes = [
    makeNode("main", "dashboard", "service", 0, "Running", "Running"),
  ];
  const toStop = getServicesToStop(1, nodes, serviceLifecycle);
  assert(toStop.length === 0, "noDependents service → never in stop list");
}

{
  const serviceLifecycle = {
    "main.cache": { startBeforeTier: 0, stopAfterTier: 2, noDependents: false },
  };
  const nodes = [
    makeNode("main", "cache", "service", 0, "Running", "Running"),
  ];
  const toStop = getServicesToStop(1, nodes, serviceLifecycle);
  assert(toStop.length === 0, "stopAfterTier=2 >= nextTier=1 → not in stop list");
}

console.log(`\nALL TESTS PASSED (${passed})`);
if (failed > 0) {
  console.log(`FAILURES: ${failed}`);
  process.exit(1);
}
