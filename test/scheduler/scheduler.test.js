import {
  buildTaskSpec,
  buildServiceSpec,
  buildVolumeSpec,
  buildExternalRefSpecs,
} from "../../src/lib/spec.js";
import {
  toOperatorResourceName,
  toOperatorVolumeName,
  translateDependsOn,
} from "../../src/lib/naming.js";
import {
  buildOperatorClients,
  getOperatorForBinding,
  getVolumeOperators,
} from "../../src/lib/infra.js";
import { OperatorClient } from "../../src/operator-client.js";

let passed = 0;
let failed = 0;
const originalFetch = globalThis.fetch;

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

function restoreFetch() {
  globalThis.fetch = originalFetch;
}

console.log("=== toOperatorResourceName ===");
{
  assert(toOperatorResourceName("wf-abc", "ingestion", "fetch-data") === "wf-abc-ingestion-fetch-data", "task name");
  assert(toOperatorResourceName("wf-xyz", "api", "dashboard") === "wf-xyz-api-dashboard", "service name");
}

console.log("\n=== toOperatorVolumeName ===");
{
  assert(toOperatorVolumeName("wf-abc", "shared-data") === "wf-abc-shared-data", "volume name");
}

console.log("\n=== translateDependsOn ===");
{
  const result = translateDependsOn(["ingestion.fetch-data", "validate"], "processing");
  assert(result[0] === "ingestion-fetch-data", "qualified dep translated");
  assert(result[1] === "processing-validate", "unqualified dep gets section prefix");
}

console.log("\n=== buildOperatorClients ===");
{
  const infraMap = new Map([
    ["local", { type: "kubernetes", endpoint: "http://localhost:8080" }],
    ["gpu", { type: "kubernetes", endpoint: "http://gpu:8080" }],
    ["same", { type: "kubernetes", endpoint: "http://localhost:8080" }],
  ]);
  const clients = buildOperatorClients(infraMap);
  assert(clients.size === 2, "deduplicates by endpoint");
  assert(clients.has("http://localhost:8080"), "has local endpoint");
  assert(clients.has("http://gpu:8080"), "has gpu endpoint");
  assert(clients.get("http://localhost:8080") instanceof OperatorClient, "returns OperatorClient instances");
}

console.log("\n=== getOperatorForBinding ===");
{
  const infraMap = new Map([
    ["local", { type: "kubernetes", endpoint: "http://localhost:8080" }],
  ]);
  const clients = buildOperatorClients(infraMap);
  const client = getOperatorForBinding("local", infraMap, clients);
  assert(client instanceof OperatorClient, "returns client for binding");

  try {
    getOperatorForBinding("nonexistent", infraMap, clients);
    assert(false, "should throw for unknown binding");
  } catch (err) {
    assert(err.message.includes("not found"), "throws for unknown binding");
  }
}

console.log("\n=== buildTaskSpec ===");
{
  const workflowId = "wf-test123";
  const node = {
    id: "ingestion.fetch-data",
    type: "task",
    section: "ingestion",
    name: "fetch-data",
    tier: 0,
    dependsOn: ["ingestion.fetch-config"],
    binding: "cluster-a",
  };
  const config = {
    sections: {
      ingestion: {
        executionMode: "sequential",
        binding: "cluster-a",
        volumes: ["shared-data"],
        volumeMounts: { "shared-data": "/mnt/shared" },
        tasks: {
          "fetch-config": { image: "alpine:3.18", command: ["sh", "-c", "echo config"] },
          "fetch-data": {
            image: "python:3.11",
            command: ["python", "fetch.py"],
            args: ["--source", "remote"],
            env: { MODE: "prod" },
            externalRefs: ["model-data"],
            volumes: ["model-vol"],
            volumeMounts: { "model-vol": "/models" },
            resources: { cpu: "500m", memory: "256Mi" },
          },
        },
      },
    },
  };
  const externalRefSpecs = new Map([
    ["model-data", { name: "model-data", protocol: "http", uri: "https://example.com/model.tar" }],
  ]);

  const spec = buildTaskSpec(workflowId, "wf-test123.ingestion.fetch-data", node, config, externalRefSpecs);

  assert(spec.taskName === "wf-test123-ingestion-fetch-data", "taskName prefixed");
  assert(spec.image === "python:3.11", "image");
  assert(spec.command[0] === "python", "command");
  assert(spec.args[0] === "--source", "args");
  assert(spec.env.MODE === "prod", "env");
  assert(spec.externalRefs.length === 1, "externalRefs resolved");
  assert(spec.externalRefs[0].name === "model-data", "externalRef name");
  assert(spec.volumes.length === 2, "volumes: section + task");
  assert(spec.volumes.includes("wf-test123-shared-data"), "includes section volume (prefixed)");
  assert(spec.volumes.includes("wf-test123-model-vol"), "includes task volume (prefixed)");
  assert(spec.volumeMounts["wf-test123-shared-data"] === "/mnt/shared", "section volumeMount (prefixed)");
  assert(spec.volumeMounts["wf-test123-model-vol"] === "/models", "task volumeMount (prefixed)");
  assert(spec.resources.cpu === "500m", "resources");
  assert(spec.dependsOn[0] === "ingestion-fetch-config", "dependsOn translated");
  assert(spec.infraRef === "cluster-a", "infraRef from binding");
}

console.log("\n=== buildTaskSpec minimal ===");
{
  const node = {
    id: "main.task1",
    type: "task",
    section: "main",
    name: "task1",
    tier: 0,
    dependsOn: [],
    binding: null,
  };
  const config = {
    sections: {
      main: {
        tasks: { task1: { image: "alpine:3.18" } },
      },
    },
  };
  const spec = buildTaskSpec("wf-min", "wf-min.main.task1", node, config, new Map());
  assert(spec.taskName === "wf-min-main-task1", "taskName");
  assert(spec.image === "alpine:3.18", "image");
  assert(!spec.command, "no command");
  assert(!spec.args, "no args");
  assert(!spec.env, "no env");
  assert(!spec.externalRefs, "no externalRefs");
  assert(!spec.volumes, "no volumes");
  assert(!spec.volumeMounts, "no volumeMounts");
  assert(!spec.resources, "no resources");
  assert(!spec.dependsOn, "no dependsOn");
  assert(!spec.infraRef, "no infraRef when no binding");
}

console.log("\n=== buildServiceSpec ===");
{
  const workflowId = "wf-test123";
  const node = {
    id: "api.dashboard",
    type: "service",
    section: "api",
    name: "dashboard",
    tier: 1,
    dependsOn: ["api.api-service"],
    binding: "cluster-a",
  };
  const config = {
    sections: {
      api: {
        binding: "cluster-a",
        services: {
          dashboard: {
            image: "node:20",
            port: 3000,
            replicas: 3,
            env: { NODE_ENV: "production" },
          },
        },
      },
    },
  };

  const spec = buildServiceSpec(workflowId, "wf-test123.api.dashboard", node, config, new Map(), "Running");

  assert(spec.serviceName === "wf-test123-api-dashboard", "serviceName prefixed");
  assert(spec.image === "node:20", "image");
  assert(spec.port === 3000, "port");
  assert(spec.replicas === 3, "replicas from config");
  assert(spec.desiredPhase === "Running", "desiredPhase");
  assert(spec.env.NODE_ENV === "production", "env");
  assert(spec.dependsOn[0] === "api-api-service", "dependsOn translated");
  assert(spec.infraRef === "cluster-a", "infraRef");
}

console.log("\n=== buildServiceSpec defaults ===");
{
  const node = {
    id: "svc.db",
    type: "service",
    section: "svc",
    name: "db",
    tier: 0,
    dependsOn: [],
    binding: null,
  };
  const config = {
    sections: { svc: { services: { db: { image: "postgres:16", port: 5432 } } } },
  };
  const spec = buildServiceSpec("wf-x", "wf-x.svc.db", node, config, new Map(), "Stopped");
  assert(spec.replicas === 1, "default replicas = 1");
  assert(spec.desiredPhase === "Stopped", "desiredPhase passed through");
}

console.log("\n=== buildVolumeSpec ===");
{
  const spec = buildVolumeSpec("wf-abc", "shared-data", { size: "10Gi", storageClass: "ssd", accessMode: "ReadWriteMany" });
  assert(spec.volumeName === "wf-abc-shared-data", "volumeName prefixed");
  assert(spec.size === "10Gi", "size");
  assert(spec.storageClass === "ssd", "storageClass");
  assert(spec.accessMode === "ReadWriteMany", "accessMode");
}

console.log("\n=== buildVolumeSpec minimal ===");
{
  const spec = buildVolumeSpec("wf-abc", "data", { size: "1Gi" });
  assert(spec.volumeName === "wf-abc-data", "volumeName");
  assert(spec.size === "1Gi", "size");
  assert(!spec.storageClass, "no storageClass");
  assert(!spec.accessMode, "no accessMode");
}

console.log("\n=== buildExternalRefSpecs ===");
{
  const specs = new Map([
    ["model-data", { name: "model-data", protocol: "http", uri: "https://example.com/model" }],
    ["s3-data", { name: "s3-data", protocol: "s3", uri: "s3://bucket/path", credentials: { secretRef: "s3-creds" } }],
  ]);

  const result = buildExternalRefSpecs(["model-data"], specs);
  assert(result.length === 1, "one ref resolved");
  assert(result[0].name === "model-data", "name");
  assert(result[0].protocol === "http", "protocol");

  const empty = buildExternalRefSpecs(undefined, specs);
  assert(empty.length === 0, "undefined refs → empty");

  const empty2 = buildExternalRefSpecs([], specs);
  assert(empty2.length === 0, "empty refs → empty");

  const missing = buildExternalRefSpecs(["nonexistent"], specs);
  assert(missing.length === 0, "unknown ref skipped");
}

console.log("\n=== getVolumeOperators ===");
{
  const config = {
    sections: {
      ingestion: {
        binding: "local",
        volumes: ["shared-data"],
        tasks: {
          "fetch-data": { image: "alpine" },
        },
      },
      processing: {
        binding: "gpu",
        tasks: {
          transform: { image: "python", volumes: ["shared-data"] },
        },
      },
    },
  };

  const infraMap = new Map([
    ["local", { type: "kubernetes", endpoint: "http://localhost:8080" }],
    ["gpu", { type: "kubernetes", endpoint: "http://gpu:8080" }],
  ]);

  const clients = buildOperatorClients(infraMap);
  const volOps = getVolumeOperators("shared-data", config, infraMap, clients);
  assert(volOps.length === 2, "volume referenced by two sections → 2 operators");
}

console.log("\n=== getVolumeOperators no references ===");
{
  const config = {
    sections: {
      main: {
        binding: "local",
        tasks: { task1: { image: "alpine" } },
      },
    },
  };
  const infraMap = new Map([
    ["local", { type: "kubernetes", endpoint: "http://localhost:8080" }],
  ]);
  const clients = buildOperatorClients(infraMap);
  const volOps = getVolumeOperators("unused-vol", config, infraMap, clients);
  assert(volOps.length === 0, "unreferenced volume → 0 operators");
}

console.log("\n=== deployWorkflow integration (mocked) ===");
{
  const calls = [];
  globalThis.fetch = async (url, opts) => {
    const method = opts?.method || "GET";
    const body = opts?.body ? JSON.parse(opts.body) : undefined;
    calls.push({ url, method, body });

    if (url.includes("/api/v1/health")) return jsonResponse({ status: "ok" });
    if (method === "POST" && url.includes("/workflows") && !url.includes("/tasks") && !url.includes("/services") && !url.includes("/volumes")) {
      return jsonResponse({ id: body.workflowName, workflowName: body.workflowName, status: { phase: "Pending" }, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }, 201);
    }
    if (method === "POST" && url.includes("/webhooks")) {
      return jsonResponse({ id: "wh-1", url: body.url, events: body.events || [], createdAt: new Date().toISOString() }, 201);
    }
    if (method === "POST" && url.includes("/tasks")) {
      return jsonResponse({ id: `wft-${body.taskName}`, taskName: body.taskName, status: { phase: "Pending" }, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }, 201);
    }
    if (method === "POST" && url.includes("/services")) {
      return jsonResponse({ id: `wfs-${body.serviceName}`, serviceName: body.serviceName, desiredPhase: body.desiredPhase, status: { phase: "Pending" }, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }, 201);
    }
    if (method === "POST" && url.includes("/volumes")) {
      return jsonResponse({ id: `wfv-${body.volumeName}`, volumeName: body.volumeName, status: { phase: "Pending" }, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() }, 201);
    }
    if (method === "DELETE") {
      return jsonResponse({ id: "deleted", status: "Deleted" });
    }
    return jsonResponse({ error: "Not found" }, 404);
  };

  const { initDb } = await import("../../src/db.js");
  const { deployWorkflow, cancelWorkflow } = await import("../../src/scheduler.js");

  const dbPath = `/tmp/scheduler-test-${Date.now()}.db`;
  const db = initDb(dbPath);

  const workflowId = "wf-test123";
  const config = {
    apiVersion: "v1",
    metadata: { name: "test-workflow" },
    infrastructure: {
      local: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" },
    },
    sections: {
      main: {
        binding: "local",
        tasks: {
          "step-one": { image: "alpine:3.18", command: ["echo", "hello"] },
          "step-two": { image: "alpine:3.18", command: ["echo", "world"], dependsOn: ["step-one"] },
        },
        services: {
          "api-service": { image: "node:20", port: 3000 },
        },
      },
    },
  };

  const { resolveDag, computeServiceLifecycle } = await import("../../src/dag.js");
  const dag = resolveDag(config);
  const serviceLifecycle = computeServiceLifecycle(dag);
  const ts = new Date().toISOString();

  db.stmts.insertWorkflow.run(workflowId, "test-workflow", "Deploying", JSON.stringify(config), JSON.stringify(dag), null, null, ts, ts);

  for (const [nodeId, node] of Object.entries(dag.nodes)) {
    const nodeIdPrefixed = `${workflowId}.${nodeId}`;
    const lifecycle = serviceLifecycle[nodeId];
    let desiredPhase = null;
    if (node.type === "service" && lifecycle) {
      desiredPhase = lifecycle.noDependents ? "Running" : null;
    }
    db.stmts.insertNode.run(
      nodeIdPrefixed, workflowId, node.type, node.section, node.name,
      "Pending", node.tier, JSON.stringify(node.dependsOn),
      node.binding, null, null, desiredPhase, ts, ts
    );
  }
  db.stmts.updateWorkflowDag.run(JSON.stringify(dag), "Deploying", ts, workflowId);

  await deployWorkflow(workflowId, db);

  const wfRow = db.stmts.getWorkflow.get(workflowId);
  assert(wfRow.status === "Running", "workflow status = Running after deploy");

  const createWfCall = calls.find(c => c.method === "POST" && c.url.endsWith("/api/v1/workflows"));
  assert(createWfCall, "createWorkflow called");
  assert(createWfCall.body.workflowName === workflowId, "workflowName = scheduler ID");

  const webhookCall = calls.find(c => c.method === "POST" && c.url.includes("/webhooks"));
  assert(webhookCall, "registerWebhook called");
  assert(webhookCall.body.url.includes("/api/v1/webhooks/operator"), "webhook URL correct");

  const taskCalls = calls.filter(c => c.method === "POST" && c.url.includes("/tasks"));
  assert(taskCalls.length === 1, "one tier-0 task created (step-one)");
  assert(taskCalls[0].body.taskName === "wf-test123-main-step-one", "tier-0 task name prefixed");

  const serviceCalls = calls.filter(c => c.method === "POST" && c.url.includes("/services"));
  assert(serviceCalls.length === 1, "one service created (api-service, noDependents)");
  assert(serviceCalls[0].body.desiredPhase === "Running", "noDependents service → Running");

  const tier1Nodes = db.stmts.listNodesByWorkflowAndTier.all(workflowId, 1);
  assert(tier1Nodes.length === 1, "one tier-1 node in DB");
  assert(tier1Nodes[0].operator_resource_id === null, "tier-1 node not deployed yet");

  const tier0Nodes = db.stmts.listNodesByWorkflowAndTier.all(workflowId, 0);
  assert(tier0Nodes.length === 2, "two tier-0 nodes (task + service)");
  const deployedTask = tier0Nodes.find(n => n.type === "task");
  assert(deployedTask.operator_resource_id === "wft-wf-test123-main-step-one", "task operator_resource_id stored");

  console.log("\n--- cancelWorkflow ---");
  calls.length = 0;
  await cancelWorkflow(workflowId, db);

  const deleteCalls = calls.filter(c => c.method === "DELETE");
  assert(deleteCalls.length >= 1, "deleteWorkflow called on operator");

  const goneRow = db.stmts.getWorkflow.get(workflowId);
  assert(!goneRow, "workflow deleted from DB after cancel");

  const { unlinkSync } = await import("fs");
  try { unlinkSync(dbPath); } catch (_) {}
  try { unlinkSync(dbPath.replace(".db", "-wal")); } catch (_) {}
  try { unlinkSync(dbPath.replace(".db", "-shm")); } catch (_) {}

  restoreFetch();
}

console.log("\n=== deployWorkflow rollback on failure ===");
{
  let callCount = 0;
  const calls = [];
  globalThis.fetch = async (url, opts) => {
    const method = opts?.method || "GET";
    const body = opts?.body ? JSON.parse(opts.body) : undefined;
    calls.push({ url, method, body });
    callCount++;

    if (method === "POST" && url.includes("/workflows") && !url.includes("/tasks") && !url.includes("/services") && !url.includes("/volumes")) {
      return jsonResponse({ id: body.workflowName, status: { phase: "Pending" } }, 201);
    }
    if (method === "POST" && url.includes("/webhooks")) {
      return jsonResponse({ id: "wh-1", url: body.url }, 201);
    }
    if (method === "POST" && url.includes("/tasks")) {
      return jsonResponse({ error: "Internal Server Error" }, 500);
    }
    if (method === "DELETE") {
      return jsonResponse({ id: "deleted" });
    }
    return jsonResponse({ error: "Not found" }, 404);
  };

  const { initDb } = await import("../../src/db.js");
  const { deployWorkflow } = await import("../../src/scheduler.js");
  const { resolveDag, computeServiceLifecycle } = await import("../../src/dag.js");

  const dbPath = `/tmp/scheduler-test-rollback-${Date.now()}.db`;
  const db = initDb(dbPath);

  const workflowId = "wf-rollback";
  const config = {
    apiVersion: "v1",
    metadata: { name: "rollback-test" },
    infrastructure: {
      local: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" },
    },
    sections: {
      main: {
        binding: "local",
        tasks: { "failing-task": { image: "alpine" } },
      },
    },
  };

  const dag = resolveDag(config);
  const serviceLifecycle = computeServiceLifecycle(dag);
  const ts = new Date().toISOString();

  db.stmts.insertWorkflow.run(workflowId, "rollback-test", "Deploying", JSON.stringify(config), JSON.stringify(dag), null, null, ts, ts);
  for (const [nodeId, node] of Object.entries(dag.nodes)) {
    const nodeIdPrefixed = `${workflowId}.${nodeId}`;
    const lifecycle = serviceLifecycle[nodeId];
    let desiredPhase = null;
    if (node.type === "service" && lifecycle) desiredPhase = lifecycle.noDependents ? "Running" : null;
    db.stmts.insertNode.run(nodeIdPrefixed, workflowId, node.type, node.section, node.name, "Pending", node.tier, JSON.stringify(node.dependsOn), node.binding, null, null, desiredPhase, ts, ts);
  }
  db.stmts.updateWorkflowDag.run(JSON.stringify(dag), "Deploying", ts, workflowId);

  try {
    await deployWorkflow(workflowId, db);
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("Deployment failed"), "throws deployment failed");
  }

  const wfRow = db.stmts.getWorkflow.get(workflowId);
  assert(wfRow.status === "Failed", "workflow status = Failed after rollback");
  assert(wfRow.deploy_error !== null, "deploy_error stored");

  const deleteCalls = calls.filter(c => c.method === "DELETE");
  assert(deleteCalls.length >= 1, "rollback: deleteWorkflow called on operator");

  const { unlinkSync } = await import("fs");
  try { unlinkSync(dbPath); } catch (_) {}
  try { unlinkSync(dbPath.replace(".db", "-wal")); } catch (_) {}
  try { unlinkSync(dbPath.replace(".db", "-shm")); } catch (_) {}

  restoreFetch();
}

console.log("\n=== cancelWorkflow undeployed ===");
{
  const { initDb } = await import("../../src/db.js");
  const { cancelWorkflow } = await import("../../src/scheduler.js");

  const dbPath = `/tmp/scheduler-test-cancel-undeployed-${Date.now()}.db`;
  const db = initDb(dbPath);

  const workflowId = "wf-undeployed";
  const ts = new Date().toISOString();
  db.stmts.insertWorkflow.run(workflowId, "undeployed", "Failed", "{}", null, null, "infra error", ts, ts);

  await cancelWorkflow(workflowId, db);

  const gone = db.stmts.getWorkflow.get(workflowId);
  assert(!gone, "undeployed workflow deleted from DB");

  const { unlinkSync } = await import("fs");
  try { unlinkSync(dbPath); } catch (_) {}
  try { unlinkSync(dbPath.replace(".db", "-wal")); } catch (_) {}
  try { unlinkSync(dbPath.replace(".db", "-shm")); } catch (_) {}

  restoreFetch();
}

console.log("\n=== deployWorkflow with volumes ===");
{
  const calls = [];
  globalThis.fetch = async (url, opts) => {
    const method = opts?.method || "GET";
    const body = opts?.body ? JSON.parse(opts.body) : undefined;
    calls.push({ url, method, body });

    if (method === "POST" && url.endsWith("/workflows")) return jsonResponse({ id: body.workflowName, status: { phase: "Pending" } }, 201);
    if (method === "POST" && url.includes("/webhooks")) return jsonResponse({ id: "wh-1", url: body.url }, 201);
    if (method === "POST" && url.includes("/volumes")) return jsonResponse({ id: `wfv-${body.volumeName}`, volumeName: body.volumeName, status: { phase: "Pending" } }, 201);
    if (method === "POST" && url.includes("/tasks")) return jsonResponse({ id: `wft-${body.taskName}`, taskName: body.taskName, status: { phase: "Pending" } }, 201);
    if (method === "DELETE") return jsonResponse({ id: "deleted" });
    return jsonResponse({ error: "Not found" }, 404);
  };

  const { initDb } = await import("../../src/db.js");
  const { deployWorkflow } = await import("../../src/scheduler.js");
  const { resolveDag, computeServiceLifecycle } = await import("../../src/dag.js");

  const dbPath = `/tmp/scheduler-test-vols-${Date.now()}.db`;
  const db = initDb(dbPath);

  const workflowId = "wf-vols";
  const config = {
    apiVersion: "v1",
    metadata: { name: "vol-test" },
    infrastructure: {
      local: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" },
    },
    volumes: {
      "shared-data": { size: "10Gi", storageClass: "ssd" },
    },
    sections: {
      main: {
        binding: "local",
        volumes: ["shared-data"],
        volumeMounts: { "shared-data": "/mnt/data" },
        tasks: { process: { image: "alpine", command: ["sh"] } },
      },
    },
  };

  const dag = resolveDag(config);
  const serviceLifecycle = computeServiceLifecycle(dag);
  const ts = new Date().toISOString();

  db.stmts.insertWorkflow.run(workflowId, "vol-test", "Deploying", JSON.stringify(config), JSON.stringify(dag), null, null, ts, ts);
  for (const [nodeId, node] of Object.entries(dag.nodes)) {
    const nodeIdPrefixed = `${workflowId}.${nodeId}`;
    const lifecycle = serviceLifecycle[nodeId];
    let desiredPhase = null;
    if (node.type === "service" && lifecycle) desiredPhase = lifecycle.noDependents ? "Running" : null;
    db.stmts.insertNode.run(nodeIdPrefixed, workflowId, node.type, node.section, node.name, "Pending", node.tier, JSON.stringify(node.dependsOn), node.binding, null, null, desiredPhase, ts, ts);
  }
  db.stmts.updateWorkflowDag.run(JSON.stringify(dag), "Deploying", ts, workflowId);

  await deployWorkflow(workflowId, db);

  const volCalls = calls.filter(c => c.method === "POST" && c.url.includes("/volumes"));
  assert(volCalls.length === 1, "one volume created on operator");
  assert(volCalls[0].body.volumeName === "wf-vols-shared-data", "volume name prefixed");

  const volRows = db.stmts.listVolumesByWorkflow.all(workflowId);
  assert(volRows.length === 1, "one volume row in DB");
  assert(volRows[0].name === "shared-data", "volume name in DB");
  assert(volRows[0].operator_resource_id === "wfv-wf-vols-shared-data", "volume operator_resource_id");

  const wfRow = db.stmts.getWorkflow.get(workflowId);
  assert(wfRow.status === "Running", "workflow Running after deploy with volumes");

  const { unlinkSync } = await import("fs");
  try { unlinkSync(dbPath); } catch (_) {}
  try { unlinkSync(dbPath.replace(".db", "-wal")); } catch (_) {}
  try { unlinkSync(dbPath.replace(".db", "-shm")); } catch (_) {}

  restoreFetch();
}

console.log("");
if (failed === 0) {
  console.log(`ALL TESTS PASSED (${passed})`);
} else {
  console.log(`SOME TESTS FAILED (${failed} failed, ${passed} passed)`);
  process.exit(1);
}
