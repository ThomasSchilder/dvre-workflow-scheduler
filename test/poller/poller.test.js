import { initDb, generateId, now } from "../../src/db.js";
import { reconcileWorkflow, checkWorkflowTimeout } from "../../src/poller.js";

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

function restoreFetch() {
  globalThis.fetch = originalFetch;
}

function createRunningWorkflow(db, config, dag, infraJson) {
  const { stmts } = db;
  const id = generateId("wf");
  const ts = now();
  const infra = infraJson || JSON.stringify([["local", { type: "kubernetes", endpoint: "http://localhost:8080" }]]);

  stmts.insertWorkflow.run(id, "test", "Running", JSON.stringify(config), JSON.stringify(dag), infra, null, ts, ts);

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

const parallelConfig = {
  metadata: { name: "poller-test" },
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

process.env.OPERATOR_RETRY_ATTEMPTS = "1";
process.env.OPERATOR_TIMEOUT_MS = "1000";

console.log("\n=== reconcileWorkflow: operator says task Succeeded, scheduler says Pending ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  const fetchAResourceId = `${wfId}-main-fetchA`;

  globalThis.fetch = async (url) => {
    if (url.includes("/tasks")) {
      return jsonResponse({
        data: [
          { id: fetchAResourceId, status: { phase: "Succeeded" } },
          { id: `${wfId}-main-fetchB`, status: { phase: "Pending" } },
        ],
      });
    }
    if (url.includes("/services")) {
      return jsonResponse({ data: [] });
    }
    if (url.includes("/volumes")) {
      return jsonResponse({ data: [] });
    }
    return jsonResponse({ id: "mock" });
  };

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  const fetchA = stmts.getNode.get(`${wfId}.main.fetchA`);
  assert(fetchA.status === "Succeeded", "fetchA status updated to Succeeded via poll");

  const fetchB = stmts.getNode.get(`${wfId}.main.fetchB`);
  assert(fetchB.status === "Pending", "fetchB still Pending (operator agrees)");

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.poll_failures === 0, "poll_failures is 0 on success");

  restoreFetch();
}

console.log("\n=== reconcileWorkflow: status already matches — no duplicate events ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  stmts.updateNodeStatus.run("Succeeded", now(), `${wfId}.main.fetchA`);
  stmts.updateNodeStatus.run("Succeeded", now(), `${wfId}.main.fetchB`);

  const eventCountBefore = stmts.listEventsByWorkflow.all(wfId).length;

  globalThis.fetch = async (url) => {
    if (url.includes("/tasks")) {
      return jsonResponse({
        data: [
          { id: `${wfId}-main-fetchA`, status: { phase: "Succeeded" } },
          { id: `${wfId}-main-fetchB`, status: { phase: "Succeeded" } },
        ],
      });
    }
    if (url.includes("/services")) return jsonResponse({ data: [] });
    if (url.includes("/volumes")) return jsonResponse({ data: [] });
    return jsonResponse({ id: "mock" });
  };

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  const eventCountAfter = stmts.listEventsByWorkflow.all(wfId).length;
  assert(eventCountAfter === eventCountBefore, "no new events when status already matches");

  restoreFetch();
}

console.log("\n=== reconcileWorkflow: operator returns 404 — workflow marked Failed ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  globalThis.fetch = async () => {
    return jsonResponse({ error: "Workflow not found" }, 404);
  };

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Failed", "workflow Failed when operator returns 404");
  assert(wf.deploy_error && wf.deploy_error.includes("not found on operator"), "deploy_error mentions not found on operator");

  restoreFetch();
}

console.log("\n=== reconcileWorkflow: operator unreachable — increment poll_failures ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  globalThis.fetch = async () => {
    throw new Error("ECONNREFUSED");
  };

  const originalMaxRetries = process.env.POLL_MAX_RETRIES;
  process.env.POLL_MAX_RETRIES = "3";

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  let wf = stmts.getWorkflow.get(wfId);
  assert(wf.poll_failures === 1, "poll_failures incremented to 1");
  assert(wf.status === "Running", "workflow still Running after 1 failure");

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  wf = stmts.getWorkflow.get(wfId);
  assert(wf.poll_failures === 2, "poll_failures incremented to 2");
  assert(wf.status === "Running", "workflow still Running after 2 failures");

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  wf = stmts.getWorkflow.get(wfId);
  assert(wf.poll_failures === 3, "poll_failures incremented to 3");
  assert(wf.status === "Failed", "workflow Failed after 3 failures");
  assert(wf.deploy_error && wf.deploy_error.includes("unreachable"), "deploy_error mentions unreachable");

  process.env.POLL_MAX_RETRIES = originalMaxRetries;
  restoreFetch();
}

console.log("\n=== reconcileWorkflow: operator recovers — poll_failures reset ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  stmts.incrementWorkflowPollFailures.run(now(), wfId);
  stmts.incrementWorkflowPollFailures.run(now(), wfId);

  globalThis.fetch = async (url) => {
    if (url.includes("/tasks")) return jsonResponse({ data: [] });
    if (url.includes("/services")) return jsonResponse({ data: [] });
    if (url.includes("/volumes")) return jsonResponse({ data: [] });
    return jsonResponse({ id: "mock" });
  };

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.poll_failures === 0, "poll_failures reset to 0 after successful poll");

  restoreFetch();
}

console.log("\n=== reconcileWorkflow: tier advancement via poll ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  stmts.updateNodeStatus.run("Succeeded", now(), `${wfId}.main.fetchB`);

  const fetchAResourceId = `${wfId}-main-fetchA`;

  let createTaskCalls = 0;
  globalThis.fetch = async (url, opts) => {
    if (url.includes("/tasks") && opts?.method === "POST") {
      createTaskCalls++;
      return jsonResponse({ id: `${wfId}-main-process` });
    }
    if (url.includes("/tasks")) {
      return jsonResponse({
        data: [
          { id: fetchAResourceId, status: { phase: "Succeeded" } },
          { id: `${wfId}-main-fetchB`, status: { phase: "Succeeded" } },
        ],
      });
    }
    if (url.includes("/services")) return jsonResponse({ data: [] });
    if (url.includes("/volumes")) return jsonResponse({ data: [] });
    return jsonResponse({ id: "mock" });
  };

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  const fetchA = stmts.getNode.get(`${wfId}.main.fetchA`);
  assert(fetchA.status === "Succeeded", "fetchA updated to Succeeded via poll");

  assert(createTaskCalls === 1, "tier-1 task deployed after tier-0 completion detected by poll");

  const process = stmts.getNode.get(`${wfId}.main.process`);
  assert(process.operator_resource_id === `${wfId}-main-process`, "tier-1 task has operator_resource_id");

  restoreFetch();
}

console.log("\n=== checkWorkflowTimeout: expired workflow marked Failed ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  const oldTimestamp = new Date(Date.now() - 600000).toISOString();
  stmts.updateWorkflowStatus.run("Running", oldTimestamp, wfId);

  const originalTimeout = process.env.WORKFLOW_TIMEOUT_MS;
  process.env.WORKFLOW_TIMEOUT_MS = "300000";

  globalThis.fetch = async () => jsonResponse({ data: [] });

  await checkWorkflowTimeout(stmts.getWorkflow.get(wfId), db);

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Failed", "workflow Failed when timeout exceeded");
  assert(wf.deploy_error && wf.deploy_error.includes("timeout"), "deploy_error mentions timeout");

  process.env.WORKFLOW_TIMEOUT_MS = originalTimeout;
  restoreFetch();
}

console.log("\n=== checkWorkflowTimeout: timeout disabled (0) — no action ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  const oldTimestamp = new Date(Date.now() - 600000).toISOString();
  stmts.updateWorkflowStatus.run("Running", oldTimestamp, wfId);

  const originalTimeout = process.env.WORKFLOW_TIMEOUT_MS;
  process.env.WORKFLOW_TIMEOUT_MS = "0";

  await checkWorkflowTimeout(stmts.getWorkflow.get(wfId), db);

  const wf = stmts.getWorkflow.get(wfId);
  assert(wf.status === "Running", "workflow still Running when timeout disabled");

  process.env.WORKFLOW_TIMEOUT_MS = originalTimeout;
  restoreFetch();
}

console.log("\n=== reconcileWorkflow: non-Running workflow skipped ===");

{
  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, parallelConfig, parallelDag);
  const { stmts } = db;

  stmts.updateWorkflowStatus.run("Succeeded", now(), wfId);

  let fetchCalled = false;
  globalThis.fetch = async () => {
    fetchCalled = true;
    return jsonResponse({});
  };

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  assert(!fetchCalled, "no operator calls for non-Running workflow");

  restoreFetch();
}

console.log("\n=== reconcileWorkflow: service status reconciliation ===");

{
  const serviceConfig = {
    metadata: { name: "service-test" },
    infrastructure: { local: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" } },
    sections: {
      main: {
        binding: "local",
        tasks: {
          worker: { image: "alpine:3.18", command: ["echo", "work"] },
        },
        services: {
          api: { image: "nginx:alpine", port: 80 },
        },
      },
    },
  };

  const serviceDag = {
    nodes: {
      "main.worker": { type: "task", section: "main", name: "worker", tier: 0, dependsOn: [], binding: "local" },
      "main.api": { type: "service", section: "main", name: "api", tier: 0, dependsOn: [], binding: "local" },
    },
    tiers: [
      { tier: 0, nodes: ["main.worker", "main.api"] },
    ],
  };

  const db = initDb(":memory:");
  const wfId = createRunningWorkflow(db, serviceConfig, serviceDag);
  const { stmts } = db;

  const apiResourceId = `${wfId}-main-api`;

  globalThis.fetch = async (url) => {
    if (url.includes("/tasks")) {
      return jsonResponse({ data: [] });
    }
    if (url.includes("/services")) {
      return jsonResponse({
        data: [
          { id: apiResourceId, status: { phase: "Running" }, desiredPhase: "Running" },
        ],
      });
    }
    if (url.includes("/volumes")) {
      return jsonResponse({ data: [] });
    }
    return jsonResponse({ id: "mock" });
  };

  await reconcileWorkflow(stmts.getWorkflow.get(wfId), db);

  const apiNode = stmts.getNode.get(`${wfId}.main.api`);
  assert(apiNode.status === "Running", "service status updated to Running via poll");

  restoreFetch();
}

console.log(`\nALL TESTS PASSED (${passed})`);
if (failed > 0) {
  console.log(`FAILURES: ${failed}`);
  process.exit(1);
}
