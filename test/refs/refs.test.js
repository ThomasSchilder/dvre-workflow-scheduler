import { resolveInfrastructure, resolveExternalRefs, resolveExternalRefSpecs, resolveTaskAsset, resolveServiceAsset, resolveWorkflowNodes } from "../../src/refs.js";

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

function mockFetch(responses) {
  const calls = [];
  globalThis.fetch = async (url, opts) => {
    calls.push({ url, opts });
    const key = `${opts?.method || "GET"} ${url}`;
    const handler = responses[key] || responses[url];
    if (handler) {
      if (typeof handler === "function") return handler(url, opts);
      return handler;
    }
    for (const [pattern, resp] of Object.entries(responses)) {
      if (url.includes(pattern)) {
        if (typeof resp === "function") return resp(url, opts);
        return resp;
      }
    }
    return new Response(JSON.stringify({ error: "Not found" }), { status: 404 });
  };
  return calls;
}

function restoreFetch() {
  globalThis.fetch = originalFetch;
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

console.log("=== resolveInfrastructure ===");

console.log("\n--- source=direct passthrough ---");
{
  restoreFetch();
  const wf = {
    infrastructure: {
      "local-k3s": { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" },
      "gpu-cluster": { source: "direct", type: "kubernetes", endpoint: "http://gpu:8080" },
    },
  };
  const result = await resolveInfrastructure(wf, "http://unused:9999");
  assert(result instanceof Map, "returns Map");
  assert(result.get("local-k3s").type === "kubernetes", "local-k3s type");
  assert(result.get("local-k3s").endpoint === "http://localhost:8080", "local-k3s endpoint");
  assert(result.get("gpu-cluster").type === "kubernetes", "gpu-cluster type");
  assert(result.get("gpu-cluster").endpoint === "http://gpu:8080", "gpu-cluster endpoint");
}

console.log("\n--- source=asset resolves cluster asset ---");
{
  const calls = mockFetch({
    "http://indexer:3001/api/assets/5": () =>
      jsonResponse({
        asset_id: "5",
        asset_type: 4,
        name: "local-cluster",
        url: "http://localhost:8080",
        protocol: 0,
        metadata: '{"type":"k3s","version":"v1.35.5+k3s1"}',
        owner: "0xabc",
        created_block: 631510,
      }),
  });

  const wf = {
    infrastructure: {
      cluster: { source: "asset", assetId: 5 },
    },
  };
  const result = await resolveInfrastructure(wf, "http://indexer:3001");
  assert(result.get("cluster").type === "k3s", "type from metadata");
  assert(result.get("cluster").endpoint === "http://localhost:8080", "endpoint from url");
  assert(result.get("cluster").assetId === 5, "assetId preserved");
  assert(result.get("cluster").assetName === "local-cluster", "assetName preserved");
  assert(calls.length === 1, "one fetch call made");
  assert(calls[0].url === "http://indexer:3001/api/assets/5", "correct URL");
  restoreFetch();
}

console.log("\n--- source=asset with non-cluster asset throws ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/1": () =>
      jsonResponse({
        asset_id: "1",
        asset_type: 0,
        name: "Hello World",
        url: "https://docs.google.com/...",
        protocol: 0,
        metadata: "{}",
      }),
  });

  const wf = {
    infrastructure: {
      data: { source: "asset", assetId: 1 },
    },
  };
  try {
    await resolveInfrastructure(wf, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("not a cluster asset"), "throws for non-cluster asset");
    assert(err.message.includes("asset_type=0"), "includes actual asset_type");
  }
  restoreFetch();
}

console.log("\n--- source=asset with missing assetId throws ---");
{
  const wf = {
    infrastructure: {
      broken: { source: "asset" },
    },
  };
  try {
    await resolveInfrastructure(wf, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("no assetId"), "throws for missing assetId");
  }
}

console.log("\n--- asset-indexer returns 404 ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/999": () =>
      jsonResponse({ error: "Asset not found" }, 404),
  });

  const wf = {
    infrastructure: {
      missing: { source: "asset", assetId: 999 },
    },
  };
  try {
    await resolveInfrastructure(wf, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("not found"), "throws for 404");
  }
  restoreFetch();
}

console.log("\n--- asset-indexer unreachable ---");
{
  mockFetch({
    "http://down:3001/api/assets/5": () => {
      throw new Error("Connection refused");
    },
  });

  const wf = {
    infrastructure: {
      cluster: { source: "asset", assetId: 5 },
    },
  };
  try {
    await resolveInfrastructure(wf, "http://down:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("Failed to fetch asset 5"), "throws for unreachable");
  }
  restoreFetch();
}

console.log("\n--- unknown source throws ---");
{
  const wf = {
    infrastructure: {
      bad: { source: "magic" },
    },
  };
  try {
    await resolveInfrastructure(wf, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("unknown source"), "throws for unknown source");
  }
}

console.log("\n--- empty infrastructure ---");
{
  const wf = { infrastructure: {} };
  const result = await resolveInfrastructure(wf, "http://indexer:3001");
  assert(result.size === 0, "empty Map for empty infrastructure");
}

console.log("\n--- missing infrastructure ---");
{
  const wf = {};
  const result = await resolveInfrastructure(wf, "http://indexer:3001");
  assert(result.size === 0, "empty Map for missing infrastructure");
}

console.log("\n--- mixed direct + asset ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/5": () =>
      jsonResponse({
        asset_id: "5",
        asset_type: 4,
        name: "gpu-cluster",
        url: "http://gpu:8080",
        protocol: 0,
        metadata: '{"type":"kubernetes"}',
      }),
  });

  const wf = {
    infrastructure: {
      local: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" },
      gpu: { source: "asset", assetId: 5 },
    },
  };
  const result = await resolveInfrastructure(wf, "http://indexer:3001");
  assert(result.size === 2, "two infra entries");
  assert(result.get("local").endpoint === "http://localhost:8080", "direct endpoint");
  assert(result.get("gpu").endpoint === "http://gpu:8080", "asset endpoint");
  restoreFetch();
}

console.log("\n--- asset with no metadata defaults to kubernetes ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/6": () =>
      jsonResponse({
        asset_id: "6",
        asset_type: 4,
        name: "bare-cluster",
        url: "http://bare:8080",
        protocol: 0,
        metadata: null,
      }),
  });

  const wf = {
    infrastructure: {
      bare: { source: "asset", assetId: 6 },
    },
  };
  const result = await resolveInfrastructure(wf, "http://indexer:3001");
  assert(result.get("bare").type === "kubernetes", "defaults to kubernetes");
  restoreFetch();
}

console.log("\n=== resolveExternalRefs ===");

console.log("\n--- passthrough ---");
{
  const wf = {
    externalRefs: {
      "model-data": { source: "asset", assetId: 42 },
      "s3-datasets": { source: "direct", protocol: "s3", uri: "s3://bucket/data" },
    },
  };
  const result = resolveExternalRefs(wf);
  assert(result["model-data"].source === "asset", "model-data preserved");
  assert(result["s3-datasets"].protocol === "s3", "s3-datasets preserved");
}

console.log("\n--- missing externalRefs ---");
{
  const wf = {};
  const result = resolveExternalRefs(wf);
  assert(Object.keys(result).length === 0, "empty object for missing externalRefs");
}

console.log("\n=== resolveExternalRefSpecs ===");

console.log("\n--- source=direct ---");
{
  const wf = {
    externalRefs: {
      "s3-data": { source: "direct", protocol: "s3", uri: "s3://bucket/path", credentials: { secretRef: "s3-creds" } },
      "http-data": { source: "direct", protocol: "http", uri: "https://example.com/data" },
    },
  };
  const result = await resolveExternalRefSpecs(wf);
  assert(result instanceof Map, "returns Map");
  assert(result.get("s3-data").name === "s3-data", "s3-data name");
  assert(result.get("s3-data").protocol === "s3", "s3-data protocol");
  assert(result.get("s3-data").uri === "s3://bucket/path", "s3-data uri");
  assert(result.get("s3-data").credentials.secretRef === "s3-creds", "s3-data credentials");
  assert(result.get("http-data").protocol === "http", "http-data protocol");
  assert(!result.get("http-data").credentials, "http-data no credentials");
}

console.log("\n--- source=asset resolves protocol ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/10": () =>
      jsonResponse({
        asset_id: "10",
        asset_type: 0,
        name: "dataset",
        url: "https://example.com/dataset.tar",
        protocol: 0,
        metadata: '{"format":"tar"}',
      }),
    "http://indexer:3001/api/assets/11": () =>
      jsonResponse({
        asset_id: "11",
        asset_type: 0,
        name: "s3-dataset",
        url: "s3://bucket/data",
        protocol: 2,
        metadata: null,
      }),
  });

  const wf = {
    externalRefs: {
      "http-ref": { source: "asset", assetId: 10 },
      "s3-ref": { source: "asset", assetId: 11 },
    },
  };
  const result = await resolveExternalRefSpecs(wf, "http://indexer:3001");
  assert(result.get("http-ref").protocol === "http", "asset protocol 0 → http");
  assert(result.get("http-ref").uri === "https://example.com/dataset.tar", "asset uri from url");
  assert(result.get("s3-ref").protocol === "s3", "asset protocol 2 → s3");
  assert(result.get("s3-ref").metadata === '{"format":"tar"}' || result.get("s3-ref").name === "s3-ref", "metadata or name present");
  restoreFetch();
}

console.log("\n--- source=asset without ASSET_INDEXER_URL throws ---");
{
  const origEnv = process.env.ASSET_INDEXER_URL;
  delete process.env.ASSET_INDEXER_URL;
  const wf = {
    externalRefs: {
      ref1: { source: "asset", assetId: 5 },
    },
  };
  try {
    await resolveExternalRefSpecs(wf);
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("ASSET_INDEXER_URL"), "throws for missing ASSET_INDEXER_URL");
  }
  process.env.ASSET_INDEXER_URL = origEnv;
}

console.log("\n--- missing externalRefs returns empty Map ---");
{
  const result = await resolveExternalRefSpecs({});
  assert(result.size === 0, "empty Map for missing externalRefs");
}

console.log("\n--- unknown source throws ---");
{
  const wf = {
    externalRefs: {
      bad: { source: "magic" },
    },
  };
  try {
    await resolveExternalRefSpecs(wf);
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("unknown source"), "throws for unknown source");
  }
}

console.log("\n=== resolveTaskAsset ===");

console.log("\n--- source=direct passthrough ---");
{
  const taskDef = { source: "direct", image: "python:3.11", command: ["python", "run.py"] };
  const result = await resolveTaskAsset(taskDef, "http://unused:3001");
  assert(result.image === "python:3.11", "image preserved");
  assert(result.command[0] === "python", "command preserved");
  assert(result.source === "direct", "source preserved");
}

console.log("\n--- no source (backward compatible) passthrough ---");
{
  const taskDef = { image: "alpine:latest", args: ["echo", "hello"] };
  const result = await resolveTaskAsset(taskDef, "http://unused:3001");
  assert(result.image === "alpine:latest", "image preserved");
  assert(result.args[0] === "echo", "args preserved");
  assert(result.source === undefined, "source absent");
}

console.log("\n--- source=asset with full metadata ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/42": () =>
      jsonResponse({
        asset_id: "42",
        asset_type: 2,
        name: "my-tool",
        url: "ghcr.io/org/my-tool:1.0",
        protocol: 0,
        metadata: '{"image":"ghcr.io/org/my-tool:1.0","command":["/bin/sh","-c"],"args":["run --input /data"],"env":{"MODE":"prod","LOG_LEVEL":"info"},"resources":{"cpu":"100m","memory":"256Mi"}}',
      }),
  });

  const taskDef = { source: "asset", assetId: 42 };
  const result = await resolveTaskAsset(taskDef, "http://indexer:3001");
  assert(result.image === "ghcr.io/org/my-tool:1.0", "image from asset metadata");
  assert(result.command[0] === "/bin/sh", "command from asset metadata");
  assert(result.args[0] === "run --input /data", "args from asset metadata");
  assert(result.env.MODE === "prod", "env MODE from asset metadata");
  assert(result.env.LOG_LEVEL === "info", "env LOG_LEVEL from asset metadata");
  assert(result.resources.cpu === "100m", "resources from asset metadata");
  assert(result.resources.memory === "256Mi", "resources memory from asset metadata");
  restoreFetch();
}

console.log("\n--- source=asset with workflow overrides ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/42": () =>
      jsonResponse({
        asset_id: "42",
        asset_type: 2,
        name: "my-tool",
        url: "ghcr.io/org/my-tool:1.0",
        protocol: 0,
        metadata: '{"image":"ghcr.io/org/my-tool:1.0","command":["/bin/sh","-c"],"args":["run"],"env":{"MODE":"prod","LOG_LEVEL":"info"}}',
      }),
  });

  const taskDef = { source: "asset", assetId: 42, image: "custom:2.0", args: ["--custom"], env: { LOG_LEVEL: "debug", EXTRA: "yes" } };
  const result = await resolveTaskAsset(taskDef, "http://indexer:3001");
  assert(result.image === "custom:2.0", "workflow image overrides asset");
  assert(result.command[0] === "/bin/sh", "command from asset (no workflow override)");
  assert(result.args[0] === "--custom", "workflow args overrides asset");
  assert(result.env.MODE === "prod", "env MODE from asset (no workflow override)");
  assert(result.env.LOG_LEVEL === "debug", "env LOG_LEVEL workflow overrides asset");
  assert(result.env.EXTRA === "yes", "env EXTRA from workflow only");
  restoreFetch();
}

console.log("\n--- source=asset with partial metadata ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/42": () =>
      jsonResponse({
        asset_id: "42",
        asset_type: 2,
        name: "my-tool",
        url: "ghcr.io/org/my-tool:1.0",
        protocol: 0,
        metadata: '{"image":"ghcr.io/org/my-tool:1.0"}',
      }),
  });

  const taskDef = { source: "asset", assetId: 42, command: ["python", "run.py"] };
  const result = await resolveTaskAsset(taskDef, "http://indexer:3001");
  assert(result.image === "ghcr.io/org/my-tool:1.0", "image from asset metadata");
  assert(result.command[0] === "python", "command from workflow override");
  assert(result.args === undefined, "args absent (neither asset nor workflow)");
  restoreFetch();
}

console.log("\n--- source=asset with non-function asset throws ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/5": () =>
      jsonResponse({
        asset_id: "5",
        asset_type: 4,
        name: "cluster",
        url: "http://cluster:8080",
        protocol: 0,
        metadata: "{}",
      }),
  });

  const taskDef = { source: "asset", assetId: 5 };
  try {
    await resolveTaskAsset(taskDef, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("not a function asset"), "throws for non-function asset");
    assert(err.message.includes("asset_type=4"), "includes actual asset_type");
  }
  restoreFetch();
}

console.log("\n--- source=asset with missing assetId throws ---");
{
  const taskDef = { source: "asset" };
  try {
    await resolveTaskAsset(taskDef, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("no assetId"), "throws for missing assetId");
  }
}

console.log("\n--- source=asset without ASSET_INDEXER_URL throws ---");
{
  const origEnv = process.env.ASSET_INDEXER_URL;
  delete process.env.ASSET_INDEXER_URL;
  const taskDef = { source: "asset", assetId: 42 };
  try {
    await resolveTaskAsset(taskDef);
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("ASSET_INDEXER_URL"), "throws for missing ASSET_INDEXER_URL");
  }
  process.env.ASSET_INDEXER_URL = origEnv;
}

console.log("\n--- source=asset with empty metadata ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/42": () =>
      jsonResponse({
        asset_id: "42",
        asset_type: 2,
        name: "empty-tool",
        url: "",
        protocol: 0,
        metadata: null,
      }),
  });

  const taskDef = { source: "asset", assetId: 42, image: "fallback:latest" };
  const result = await resolveTaskAsset(taskDef, "http://indexer:3001");
  assert(result.image === "fallback:latest", "workflow image used when no asset metadata");
  restoreFetch();
}

console.log("\n--- unknown source throws ---");
{
  const taskDef = { source: "magic" };
  try {
    await resolveTaskAsset(taskDef, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("unknown source"), "throws for unknown source");
  }
}

console.log("\n=== resolveServiceAsset ===");

console.log("\n--- source=direct passthrough ---");
{
  const serviceDef = { source: "direct", image: "nginx:latest", port: 8080, replicas: 3 };
  const result = await resolveServiceAsset(serviceDef, "http://unused:3001");
  assert(result.image === "nginx:latest", "image preserved");
  assert(result.port === 8080, "port preserved");
  assert(result.replicas === 3, "replicas preserved");
}

console.log("\n--- no source passthrough ---");
{
  const serviceDef = { image: "nginx:latest", port: 80 };
  const result = await resolveServiceAsset(serviceDef, "http://unused:3001");
  assert(result.image === "nginx:latest", "image preserved");
  assert(result.port === 80, "port preserved");
}

console.log("\n--- source=asset with full metadata ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/55": () =>
      jsonResponse({
        asset_id: "55",
        asset_type: 2,
        name: "api-server",
        url: "ghcr.io/org/api:2.0",
        protocol: 0,
        metadata: '{"image":"ghcr.io/org/api:2.0","env":{"PORT":"3000","DEBUG":"false"},"resources":{"cpu":"250m","memory":"512Mi"}}',
      }),
  });

  const serviceDef = { source: "asset", assetId: 55, port: 8080, replicas: 2 };
  const result = await resolveServiceAsset(serviceDef, "http://indexer:3001");
  assert(result.image === "ghcr.io/org/api:2.0", "image from asset metadata");
  assert(result.port === 8080, "port from workflow (not asset)");
  assert(result.replicas === 2, "replicas from workflow (not asset)");
  assert(result.env.PORT === "3000", "env PORT from asset metadata");
  assert(result.env.DEBUG === "false", "env DEBUG from asset metadata");
  assert(result.resources.cpu === "250m", "resources from asset metadata");
  restoreFetch();
}

console.log("\n--- source=asset with env override ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/55": () =>
      jsonResponse({
        asset_id: "55",
        asset_type: 2,
        name: "api-server",
        url: "ghcr.io/org/api:2.0",
        protocol: 0,
        metadata: '{"image":"ghcr.io/org/api:2.0","env":{"PORT":"3000","DEBUG":"false"}}',
      }),
  });

  const serviceDef = { source: "asset", assetId: 55, port: 8080, env: { DEBUG: "true" } };
  const result = await resolveServiceAsset(serviceDef, "http://indexer:3001");
  assert(result.env.PORT === "3000", "env PORT from asset (no workflow override)");
  assert(result.env.DEBUG === "true", "env DEBUG workflow overrides asset");
  restoreFetch();
}

console.log("\n--- source=asset with non-function asset throws ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/1": () =>
      jsonResponse({
        asset_id: "1",
        asset_type: 0,
        name: "dataset",
        url: "https://example.com/data",
        protocol: 0,
        metadata: "{}",
      }),
  });

  const serviceDef = { source: "asset", assetId: 1, port: 8080 };
  try {
    await resolveServiceAsset(serviceDef, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("not a function asset"), "throws for non-function asset");
  }
  restoreFetch();
}

console.log("\n--- source=asset with missing assetId throws ---");
{
  const serviceDef = { source: "asset", port: 8080 };
  try {
    await resolveServiceAsset(serviceDef, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("no assetId"), "throws for missing assetId");
  }
}

console.log("\n--- unknown source throws ---");
{
  const serviceDef = { source: "magic", port: 8080 };
  try {
    await resolveServiceAsset(serviceDef, "http://indexer:3001");
    assert(false, "should have thrown");
  } catch (err) {
    assert(err.message.includes("unknown source"), "throws for unknown source");
  }
}

console.log("\n=== resolveWorkflowNodes ===");

console.log("\n--- resolves tasks and services across sections ---");
{
  mockFetch({
    "http://indexer:3001/api/assets/42": () =>
      jsonResponse({
        asset_id: "42",
        asset_type: 2,
        name: "my-tool",
        url: "ghcr.io/org/my-tool:1.0",
        protocol: 0,
        metadata: '{"image":"ghcr.io/org/my-tool:1.0","command":["run"]}',
      }),
    "http://indexer:3001/api/assets/55": () =>
      jsonResponse({
        asset_id: "55",
        asset_type: 2,
        name: "api-server",
        url: "ghcr.io/org/api:2.0",
        protocol: 0,
        metadata: '{"image":"ghcr.io/org/api:2.0","env":{"PORT":"3000"}}',
      }),
  });

  const workflow = {
    sections: {
      process: {
        tasks: {
          "asset-task": { source: "asset", assetId: 42 },
          "direct-task": { image: "alpine:latest", command: ["echo"] },
        },
      },
      serve: {
        services: {
          "asset-svc": { source: "asset", assetId: 55, port: 8080 },
          "direct-svc": { image: "nginx:latest", port: 80 },
        },
      },
    },
  };

  const resolved = await resolveWorkflowNodes(workflow, "http://indexer:3001");
  assert(resolved.process.tasks["asset-task"].image === "ghcr.io/org/my-tool:1.0", "asset task resolved");
  assert(resolved.process.tasks["asset-task"].command[0] === "run", "asset task command from metadata");
  assert(resolved.process.tasks["direct-task"].image === "alpine:latest", "direct task unchanged");
  assert(resolved.serve.services["asset-svc"].image === "ghcr.io/org/api:2.0", "asset service resolved");
  assert(resolved.serve.services["asset-svc"].env.PORT === "3000", "asset service env from metadata");
  assert(resolved.serve.services["asset-svc"].port === 8080, "asset service port from workflow");
  assert(resolved.serve.services["direct-svc"].image === "nginx:latest", "direct service unchanged");
  restoreFetch();
}

console.log("\n--- no asset sources returns sections unchanged ---");
{
  const workflow = {
    sections: {
      main: {
        tasks: { t: { image: "alpine:latest" } },
      },
    },
  };
  const resolved = await resolveWorkflowNodes(workflow, "http://unused:3001");
  assert(resolved.main.tasks.t.image === "alpine:latest", "task unchanged");
}

console.log("");
if (failed === 0) {
  console.log(`ALL TESTS PASSED (${passed})`);
} else {
  console.log(`SOME TESTS FAILED (${failed} failed, ${passed} passed)`);
  process.exit(1);
}
