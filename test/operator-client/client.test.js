import { OperatorClient } from "../../src/operator-client.js";

let passed = 0;
let failed = 0;
const originalFetch = globalThis.fetch;
let mockCalls = [];
let mockResponses = {};

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

function setupMock(responses) {
  mockCalls = [];
  mockResponses = responses || {};
  globalThis.fetch = async (url, opts) => {
    mockCalls.push({ url, method: opts?.method, body: opts?.body ? JSON.parse(opts.body) : undefined });
    const key = `${opts?.method || "GET"} ${url}`;
    if (mockResponses[key]) {
      const r = mockResponses[key];
      return typeof r === "function" ? r(url, opts) : r;
    }
    return jsonResponse({ error: "Not found" }, 404);
  };
}

function restoreFetch() {
  globalThis.fetch = originalFetch;
}

const BASE = "http://operator:8080";

console.log("=== OperatorClient constructor ===");

console.log("\n--- requires baseUrl ---");
{
  try {
    new OperatorClient({});
    assert(false, "should throw without baseUrl");
  } catch (err) {
    assert(err.message.includes("baseUrl"), "throws without baseUrl");
  }
}

console.log("\n--- strips trailing slash ---");
{
  const c = new OperatorClient({ baseUrl: "http://op:8080/" });
  assert(c.baseUrl === "http://op:8080", "trailing slash stripped");
}

console.log("=== healthCheck ===");

console.log("\n--- healthCheck returns data ---");
{
  setupMock({
    [`GET ${BASE}/api/v1/health`]: jsonResponse({ status: "ok", version: "0.1.0" }),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.healthCheck();
  assert(result.status === "ok", "health status ok");
  assert(result.version === "0.1.0", "health version");
  restoreFetch();
}

console.log("=== createWorkflow ===");

console.log("\n--- createWorkflow sends POST ---");
{
  setupMock({
    [`POST ${BASE}/api/v1/workflows`]: (_, opts) => {
      const body = JSON.parse(opts.body);
      return jsonResponse({ id: "wf-123", workflowName: body.workflowName, status: { phase: "Pending" } }, 201);
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.createWorkflow({ workflowName: "test-wf" });
  assert(result.id === "wf-123", "workflow id");
  assert(mockCalls[0].method === "POST", "method is POST");
  assert(mockCalls[0].body.workflowName === "test-wf", "workflowName in body");
  restoreFetch();
}

console.log("=== createTask ===");

console.log("\n--- createTask sends POST with correct path ---");
{
  setupMock({
    [`POST ${BASE}/api/v1/workflows/wf-123/tasks`]: (_, opts) => {
      const body = JSON.parse(opts.body);
      return jsonResponse({ id: "t-1", taskName: body.taskName, workflowId: "wf-123", status: { phase: "Pending" } }, 201);
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.createTask("wf-123", { taskName: "fetch", image: "alpine" });
  assert(result.id === "t-1", "task id");
  assert(mockCalls[0].url === `${BASE}/api/v1/workflows/wf-123/tasks`, "correct URL");
  assert(mockCalls[0].body.taskName === "fetch", "taskName in body");
  restoreFetch();
}

console.log("=== createService ===");

console.log("\n--- createService sends POST with desiredPhase ---");
{
  setupMock({
    [`POST ${BASE}/api/v1/workflows/wf-123/services`]: (_, opts) => {
      const body = JSON.parse(opts.body);
      return jsonResponse({ id: "s-1", serviceName: body.serviceName, workflowId: "wf-123", desiredPhase: body.desiredPhase, status: { phase: "Pending" } }, 201);
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.createService("wf-123", { serviceName: "api", image: "nginx", port: 80, desiredPhase: "Running" });
  assert(result.id === "s-1", "service id");
  assert(mockCalls[0].body.desiredPhase === "Running", "desiredPhase in body");
  restoreFetch();
}

console.log("=== createVolume ===");

console.log("\n--- createVolume sends POST ---");
{
  setupMock({
    [`POST ${BASE}/api/v1/workflows/wf-123/volumes`]: (_, opts) => {
      const body = JSON.parse(opts.body);
      return jsonResponse({ id: "v-1", volumeName: body.volumeName, workflowId: "wf-123", status: { phase: "Pending" } }, 201);
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.createVolume("wf-123", { volumeName: "data", size: "5Gi" });
  assert(result.id === "v-1", "volume id");
  assert(mockCalls[0].body.volumeName === "data", "volumeName in body");
  restoreFetch();
}

console.log("=== patchServiceDesiredPhase ===");

console.log("\n--- patchService sends PATCH with desiredPhase ---");
{
  setupMock({
    [`PATCH ${BASE}/api/v1/workflows/wf-123/services/s-1`]: (_, opts) => {
      const body = JSON.parse(opts.body);
      return jsonResponse({ id: "s-1", desiredPhase: body.desiredPhase, status: { phase: "Stopped" } });
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.patchServiceDesiredPhase("wf-123", "s-1", "Stopped");
  assert(mockCalls[0].method === "PATCH", "method is PATCH");
  assert(mockCalls[0].body.desiredPhase === "Stopped", "desiredPhase in body");
  restoreFetch();
}

console.log("=== registerWebhook ===");

console.log("\n--- registerWebhook sends POST ---");
{
  setupMock({
    [`POST ${BASE}/api/v1/webhooks`]: (_, opts) => {
      const body = JSON.parse(opts.body);
      return jsonResponse({ id: "wh-1", url: body.url, events: body.events || [] }, 201);
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.registerWebhook({ url: "http://scheduler:3000/api/v1/webhooks/operator" });
  assert(result.id === "wh-1", "webhook id");
  assert(mockCalls[0].body.url === "http://scheduler:3000/api/v1/webhooks/operator", "url in body");
  restoreFetch();
}

console.log("=== GET methods ===");

console.log("\n--- listWorkflows ---");
{
  setupMock({
    [`GET ${BASE}/api/v1/workflows`]: jsonResponse({ data: [{ id: "wf-1" }, { id: "wf-2" }] }),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.listWorkflows();
  assert(result.data.length === 2, "two workflows");
  restoreFetch();
}

console.log("\n--- listWorkflows with phase filter ---");
{
  setupMock({
    [`GET ${BASE}/api/v1/workflows?phase=Running`]: jsonResponse({ data: [{ id: "wf-1" }] }),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.listWorkflows("Running");
  assert(mockCalls[0].url.includes("phase=Running"), "phase query param");
  restoreFetch();
}

console.log("\n--- getWorkflow ---");
{
  setupMock({
    [`GET ${BASE}/api/v1/workflows/wf-123`]: jsonResponse({ id: "wf-123", status: { phase: "Running" } }),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.getWorkflow("wf-123");
  assert(result.id === "wf-123", "workflow id");
  restoreFetch();
}

console.log("\n--- deleteWorkflow ---");
{
  setupMock({
    [`DELETE ${BASE}/api/v1/workflows/wf-123`]: jsonResponse({ id: "wf-123", status: { phase: "Failed" } }),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.deleteWorkflow("wf-123");
  assert(mockCalls[0].method === "DELETE", "method is DELETE");
  restoreFetch();
}

console.log("\n--- listTasks / listServices / listVolumes with phase ---");
{
  setupMock({
    [`GET ${BASE}/api/v1/workflows/wf-1/tasks?phase=Succeeded`]: jsonResponse({ data: [{ id: "t-1" }] }),
    [`GET ${BASE}/api/v1/workflows/wf-1/services?phase=Running`]: jsonResponse({ data: [{ id: "s-1" }] }),
    [`GET ${BASE}/api/v1/workflows/wf-1/volumes?phase=Bound`]: jsonResponse({ data: [{ id: "v-1" }] }),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const tasks = await client.listTasks("wf-1", "Succeeded");
  const services = await client.listServices("wf-1", "Running");
  const volumes = await client.listVolumes("wf-1", "Bound");
  assert(tasks.data.length === 1, "listTasks with phase");
  assert(services.data.length === 1, "listServices with phase");
  assert(volumes.data.length === 1, "listVolumes with phase");
  restoreFetch();
}

console.log("\n--- deleteTask / deleteService / deleteVolume / deleteWebhook ---");
{
  setupMock({
    [`DELETE ${BASE}/api/v1/workflows/wf-1/tasks/t-1`]: jsonResponse({ id: "t-1" }),
    [`DELETE ${BASE}/api/v1/workflows/wf-1/services/s-1`]: jsonResponse({ id: "s-1" }),
    [`DELETE ${BASE}/api/v1/workflows/wf-1/volumes/v-1`]: jsonResponse({ id: "v-1" }),
    [`DELETE ${BASE}/api/v1/webhooks/wh-1`]: jsonResponse({ id: "wh-1" }),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  await client.deleteTask("wf-1", "t-1");
  await client.deleteService("wf-1", "s-1");
  await client.deleteVolume("wf-1", "v-1");
  await client.deleteWebhook("wh-1");
  assert(mockCalls.filter((c) => c.method === "DELETE").length === 4, "4 DELETE calls");
  restoreFetch();
}

console.log("\n--- listWebhooks ---");
{
  setupMock({
    [`GET ${BASE}/api/v1/webhooks`]: jsonResponse({ data: [{ id: "wh-1" }] }),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 1 });
  const result = await client.listWebhooks();
  assert(result.data.length === 1, "listWebhooks");
  restoreFetch();
}

console.log("=== Error handling ===");

console.log("\n--- 4xx error thrown immediately ---");
{
  let callCount = 0;
  setupMock({
    [`POST ${BASE}/api/v1/workflows`]: () => {
      callCount++;
      return jsonResponse({ error: "Invalid request", details: "workflowName is required" }, 400);
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 3 });
  try {
    await client.createWorkflow({});
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("Invalid request"), "4xx error message");
    assert(err.status === 400, "4xx status preserved");
    assert(callCount === 1, "4xx not retried");
  }
  restoreFetch();
}

console.log("\n--- 5xx error retried then throws ---");
{
  let callCount = 0;
  setupMock({
    [`GET ${BASE}/api/v1/health`]: () => {
      callCount++;
      return jsonResponse({ error: "Internal error" }, 500);
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 2, timeout: 500 });
  try {
    await client.healthCheck();
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("Server error"), "5xx error message");
    assert(callCount === 2, "5xx retried 2 times");
  }
  restoreFetch();
}

console.log("\n--- 5xx then success ---");
{
  let callCount = 0;
  setupMock({
    [`GET ${BASE}/api/v1/health`]: () => {
      callCount++;
      if (callCount < 3) return jsonResponse({ error: "Internal error" }, 500);
      return jsonResponse({ status: "ok" });
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 3, timeout: 500 });
  const result = await client.healthCheck();
  assert(result.status === "ok", "succeeded after retries");
  assert(callCount === 3, "3 calls total");
  restoreFetch();
}

console.log("\n--- 404 throws immediately ---");
{
  setupMock({
    [`GET ${BASE}/api/v1/workflows/wf-missing`]: () =>
      jsonResponse({ error: "Workflow not found" }, 404),
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 3 });
  try {
    await client.getWorkflow("wf-missing");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.status === 404, "404 status");
    assert(err.message.includes("not found"), "404 error message");
  }
  restoreFetch();
}

console.log("\n--- network error retried ---");
{
  let callCount = 0;
  setupMock({
    [`GET ${BASE}/api/v1/health`]: () => {
      callCount++;
      if (callCount < 2) throw new Error("ECONNREFUSED");
      return jsonResponse({ status: "ok" });
    },
  });
  const client = new OperatorClient({ baseUrl: BASE, retryAttempts: 3, timeout: 500 });
  const result = await client.healthCheck();
  assert(result.status === "ok", "recovered from network error");
  assert(callCount === 2, "2 calls after network error");
  restoreFetch();
}

console.log("");
if (failed === 0) {
  console.log(`ALL TESTS PASSED (${passed})`);
} else {
  console.log(`SOME TESTS FAILED (${failed} failed, ${passed} passed)`);
  process.exit(1);
}
