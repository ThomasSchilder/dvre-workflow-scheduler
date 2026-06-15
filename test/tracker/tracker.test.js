import { initDb, generateId, now } from "../../src/db.js";
import { handleWebhook, advancingWorkflows } from "../../src/tracker.js";

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

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const originalFetch = globalThis.fetch;

function setupMockFetch(responses) {
  const calls = [];
  globalThis.fetch = async (url, opts) => {
    calls.push({ url, method: opts?.method, body: opts?.body ? JSON.parse(opts.body) : null });
    const response = responses.shift();
    if (response) return response;
    return jsonResponse({ id: "mock-id" });
  };
  return calls;
}

function restoreFetch() {
  globalThis.fetch = originalFetch;
}

function createTestWorkflow(db, config, dag) {
  const { stmts } = db;
  const id = generateId("wf");
  const ts = now();
  stmts.insertWorkflow.run(id, "test", "Running", JSON.stringify(config), JSON.stringify(dag),
    JSON.stringify([["local", { type: "kubernetes", endpoint: "http://localhost:8080" }]]), null, ts, ts);

  for (const [nodeId, node] of Object.entries(dag.nodes)) {
    const prefixedId = `${id}.${nodeId}`;
    let desiredPhase = null;
    const operatorResourceId = node.tier === 0 ? `${id}-${node.section}-${node.name}` : null;
    if (node.type === "service" && node.tier === 0) {
      desiredPhase = "Running";
    }
    stmts.insertNode.run(prefixedId, id, node.type, node.section, node.name,
      "Pending", node.tier, JSON.stringify(node.dependsOn),
      node.binding, id, operatorResourceId, desiredPhase, ts, ts);
  }

  return id;
}

const sequential3Config = {
  metadata: { name: "test" },
  infrastructure: { local: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" } },
  sections: {
    main: {
      executionMode: "sequential",
      binding: "local",
      tasks: {
        step1: { image: "alpine:3.18", command: ["echo", "1"] },
        step2: { image: "alpine:3.18", command: ["echo", "2"] },
        step3: { image: "alpine:3.18", command: ["echo", "3"] },
      },
    },
  },
};

const sequential3Dag = {
  nodes: {
    "main.step1": { type: "task", section: "main", name: "step1", tier: 0, dependsOn: [], binding: "local" },
    "main.step2": { type: "task", section: "main", name: "step2", tier: 1, dependsOn: ["main.step1"], binding: "local" },
    "main.step3": { type: "task", section: "main", name: "step3", tier: 2, dependsOn: ["main.step2"], binding: "local" },
  },
  tiers: [
    { tier: 0, nodes: ["main.step1"] },
    { tier: 1, nodes: ["main.step2"] },
    { tier: 2, nodes: ["main.step3"] },
  ],
};

const parallelConfig = {
  metadata: { name: "parallel-test" },
  infrastructure: { local: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" } },
  sections: {
    main: {
      binding: "local",
      tasks: {
        fetchA: { image: "alpine:3.18", command: ["echo", "A"] },
        fetchB: { image: "alpine:3.18", command: ["echo", "B"] },
        process: { image: "alpine:3.18", command: ["echo", "process"], dependsOn: ["fetchA", "fetchB"] },
      },
    },
  },
};

const parallelDag = {
  nodes: {
    "main.fetchA": { type: "task", section: "main", name: "fetchA", tier: 0, dependsOn: [], binding: "local" },
    "main.fetchB": { type: "task", section: "main", name: "fetchB", tier: 0, dependsOn: [], binding: "local" },
    "main.process": { type: "task", section: "main", name: "process", tier: 1, dependsOn: ["main.fetchA", "main.fetchB"], binding: "local" },
  },
  tiers: [
    { tier: 0, nodes: ["main.fetchA", "main.fetchB"] },
    { tier: 1, nodes: ["main.process"] },
  ],
};

console.log("=== handleWebhook: task.succeeded — tier not complete ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  const fetchAResourceId = `${wfId}-main-fetchA`;

  setupMockFetch([]);

  await handleWebhook(db, "task.succeeded", wfId, fetchAResourceId, "task", { phase: "Succeeded" });

  const fetchA = stmts.getNode.get(`${wfId}.main.fetchA`);
  assert(fetchA.status === "Succeeded", "fetchA status updated to Succeeded");

  const fetchB = stmts.getNode.get(`${wfId}.main.fetchB`);
  assert(fetchB.status === "Pending", "fetchB still Pending");

  const process = stmts.getNode.get(`${wfId}.main.process`);
  assert(process.operator_resource_id === null, "tier-1 task not deployed (tier 0 not complete)");

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Running", "workflow still Running (tier 0 not complete)");

  restoreFetch();
}

console.log("\n=== handleWebhook: task.succeeded — tier complete, advance to next tier ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);
  const { stmts } = db;

  const step1ResourceId = `${wfId}-main-step1`;

  const calls = setupMockFetch([
    jsonResponse({ id: "mock-task-2" }),
  ]);

  await handleWebhook(db, "task.succeeded", wfId, step1ResourceId, "task", { phase: "Succeeded" });

  const step2NodeId = `${wfId}.main.step2`;
  const step2 = stmts.getNode.get(step2NodeId);
  assert(step2.operator_resource_id === "mock-task-2", "tier-1 task deployed via operator");

  const createdCall = calls.find(c => c.url.includes("/tasks") && c.method === "POST");
  assert(!!createdCall, "createTask called for tier-1");

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Running", "workflow still Running after advancing to tier 1");

  restoreFetch();
}

console.log("\n=== handleWebhook: task.succeeded — final tier complete → workflow Succeeded ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);
  const { stmts } = db;

  const step1ResourceId = `${wfId}-main-step1`;
  const step2NodeId = `${wfId}.main.step2`;
  const step2ResourceId = `${wfId}-main-step2`;
  const step3NodeId = `${wfId}.main.step3`;
  const step3ResourceId = `${wfId}-main-step3`;

  setupMockFetch([
    jsonResponse({ id: "mock-task-2" }),
    jsonResponse({ id: "mock-task-3" }),
  ]);

  await handleWebhook(db, "task.succeeded", wfId, step1ResourceId, "task", { phase: "Succeeded" });

  advancingWorkflows.clear();

  const step2 = stmts.getNode.get(step2NodeId);
  const step2OpId = step2.operator_resource_id;

  await handleWebhook(db, "task.succeeded", wfId, step2OpId, "task", { phase: "Succeeded" });

  advancingWorkflows.clear();

  const step3 = stmts.getNode.get(step3NodeId);
  const step3OpId = step3.operator_resource_id;

  await handleWebhook(db, "task.succeeded", wfId, step3OpId, "task", { phase: "Succeeded" });

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Succeeded", "workflow Succeeded after final tier complete");

  restoreFetch();
}

console.log("\n=== handleWebhook: task.failed → workflow Failed ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);
  const { stmts } = db;

  const step1ResourceId = `${wfId}-main-step1`;

  setupMockFetch([
    jsonResponse({ ok: true }),
  ]);

  await handleWebhook(db, "task.failed", wfId, step1ResourceId, "task", { phase: "Failed" });

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Failed", "workflow Failed after task.failed");

  restoreFetch();
}

console.log("\n=== handleWebhook: service.failed → workflow Failed ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);
  const { stmts } = db;

  const svcResourceId = `${wfId}-main-step1`;

  setupMockFetch([]);

  await handleWebhook(db, "service.failed", wfId, svcResourceId, "service", { phase: "Failed" });

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Failed", "workflow Failed after service.failed");

  restoreFetch();
}

console.log("\n=== handleWebhook: terminal workflow — no advancement ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);
  const { stmts } = db;

  stmts.updateWorkflowStatus.run("Succeeded", now(), wfId);

  const step1ResourceId = `${wfId}-main-step1`;
  let fetchCalled = false;
  globalThis.fetch = async () => { fetchCalled = true; return jsonResponse({}); };

  await handleWebhook(db, "task.succeeded", wfId, step1ResourceId, "task", { phase: "Succeeded" });

  assert(!fetchCalled, "no operator calls for terminal workflow");

  restoreFetch();
}

console.log("\n=== handleWebhook: volume event — status update only ===");

{
  const db = initDb(":memory:");
  const { stmts } = db;
  const wfId = generateId("wf");
  const ts = now();
  stmts.insertWorkflow.run(wfId, "test", "Running", "{}", null, null, null, ts, ts);
  stmts.insertVolume.run("vol-1", wfId, "shared-data", "Pending", `${wfId}-shared-data`, "10Gi", null, null, ts, ts);

  setupMockFetch([]);

  await handleWebhook(db, "volume.bound", wfId, `${wfId}-shared-data`, "volume", { phase: "Bound" });

  const vol = stmts.getVolumeByOperatorResourceId.get(`${wfId}-shared-data`);
  assert(vol.status === "Bound", "volume status updated to Bound");

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Running", "workflow still Running after volume event");

  restoreFetch();
}

console.log("\n=== handleWebhook: unknown resourceId — no crash ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);

  setupMockFetch([]);

  let error = null;
  try {
    await handleWebhook(db, "task.succeeded", wfId, "nonexistent-resource", "task", { phase: "Succeeded" });
  } catch (e) {
    error = e;
  }

  assert(error === null, "no crash for unknown resourceId");

  restoreFetch();
}

console.log("\n=== handleWebhook: concurrency lock prevents double advancement ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);

  advancingWorkflows.add(wfId);

  const step1ResourceId = `${wfId}-main-step1`;
  let fetchCalled = false;
  globalThis.fetch = async () => { fetchCalled = true; return jsonResponse({}); };

  await handleWebhook(db, "task.succeeded", wfId, step1ResourceId, "task", { phase: "Succeeded" });

  assert(!fetchCalled, "no operator calls when workflow is locked");

  advancingWorkflows.clear();
  restoreFetch();
}

console.log("\n=== concurrency lock: second webhook skipped while first is in-flight ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  const fetchAResourceId = `${wfId}-main-fetchA`;
  const fetchBResourceId = `${wfId}-main-fetchB`;

  stmts.updateNodeStatus.run("Succeeded", now(), `${wfId}.main.fetchB`);

  let createTaskCalls = 0;
  let firstFetchDelay = null;

  globalThis.fetch = async (url, opts) => {
    if (url.includes("/tasks") && opts?.method === "POST") {
      createTaskCalls++;
      if (createTaskCalls === 1 && firstFetchDelay) {
        await firstFetchDelay;
      }
      return jsonResponse({ id: "mock-tier1-task" });
    }
    return jsonResponse({ id: "mock-id" });
  };

  firstFetchDelay = new Promise((resolve) => setTimeout(resolve, 300));

  const webhookA = handleWebhook(db, "task.succeeded", wfId, fetchAResourceId, "task", { phase: "Succeeded" });

  await new Promise((r) => setTimeout(r, 100));

  assert(advancingWorkflows.has(wfId), "lock is held while advanceTier is in-flight");

  const webhookB = handleWebhook(db, "task.succeeded", wfId, fetchBResourceId, "task", { phase: "Succeeded" });

  await webhookB;

  const callsAfterWebhookB = createTaskCalls;

  await webhookA;

  await new Promise((r) => setTimeout(r, 50));

  assert(!advancingWorkflows.has(wfId), "lock released after advancement completes");

  assert(createTaskCalls === 1, `createTask called only once (got ${createTaskCalls}, second webhook skipped)`);

  const step2 = stmts.getNode.get(`${wfId}.main.process`);
  assert(step2.operator_resource_id === "mock-tier1-task", "tier-1 task deployed exactly once");

  restoreFetch();
}

console.log("\n=== handleWebhook: workflow.completed does NOT prematurely mark Succeeded ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);
  const { stmts } = db;

  const step1ResourceId = `${wfId}-main-step1`;

  const calls = setupMockFetch([
    jsonResponse({ id: "mock-task-2" }),
  ]);

  await handleWebhook(db, "task.succeeded", wfId, step1ResourceId, "task", { phase: "Succeeded" });

  advancingWorkflows.clear();

  await handleWebhook(db, "workflow.completed", wfId, null, "workflow", { phase: "Succeeded" });

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Running", "workflow NOT Succeeded when higher tiers remain (still Running)");

  const step2 = stmts.getNode.get(`${wfId}.main.step2`);
  assert(step2.operator_resource_id === "mock-task-2", "tier-1 task deployed after workflow.completed triggered advancement");

  const createdCall = calls.find(c => c.url.includes("/tasks") && c.method === "POST");
  assert(!!createdCall, "createTask called for tier-1 after workflow.completed");

  restoreFetch();
}

console.log("\n=== handleWebhook: workflow.completed marks Succeeded when all tiers complete ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);
  const { stmts } = db;

  const step1ResourceId = `${wfId}-main-step1`;
  const step2NodeId = `${wfId}.main.step2`;
  const step3NodeId = `${wfId}.main.step3`;

  setupMockFetch([
    jsonResponse({ id: "mock-task-2" }),
    jsonResponse({ id: "mock-task-3" }),
  ]);

  await handleWebhook(db, "task.succeeded", wfId, step1ResourceId, "task", { phase: "Succeeded" });

  advancingWorkflows.clear();

  const step2 = stmts.getNode.get(step2NodeId);
  await handleWebhook(db, "task.succeeded", wfId, step2.operator_resource_id, "task", { phase: "Succeeded" });

  advancingWorkflows.clear();

  const step3 = stmts.getNode.get(step3NodeId);
  await handleWebhook(db, "task.succeeded", wfId, step3.operator_resource_id, "task", { phase: "Succeeded" });

  advancingWorkflows.clear();

  const wfBefore = stmts.getWorkflow.get(wfId);
  assert(wfBefore.status === "Succeeded", "workflow already Succeeded via task.succeeded");

  await handleWebhook(db, "workflow.completed", wfId, null, "workflow", { phase: "Succeeded" });

  const wfAfter = stmts.getWorkflow.get(wfId);
  assert(wfAfter.status === "Succeeded", "workflow stays Succeeded after workflow.completed");
}

console.log("\n=== handleWebhook: workflow.failed marks workflow Failed ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);

  setupMockFetch([]);

  await handleWebhook(db, "workflow.failed", wfId, null, "workflow", { phase: "Failed" });

  const { stmts } = db;
  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Failed", "workflow Failed after workflow.failed event");

  restoreFetch();
}

console.log("\n=== advanceTier: error sets workflow Failed ===");

{
  const db = initDb(":memory:");
  const wfId = createTestWorkflow(db, sequential3Config, sequential3Dag);
  const { stmts } = db;
  const step1ResourceId = `${wfId}-main-step1`;

  globalThis.fetch = async () => {
    return new Response("internal error", { status: 500 });
  };

  await handleWebhook(db, "task.succeeded", wfId, step1ResourceId, "task", { phase: "Succeeded" });

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Failed", "workflow Failed when advanceTier operator call fails");

  restoreFetch();
}

console.log(`\nALL TESTS PASSED (${passed})`);
if (failed > 0) {
  console.log(`FAILURES: ${failed}`);
  process.exit(1);
}
