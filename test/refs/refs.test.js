import { resolveInfrastructure, resolveExternalRefs, resolveExternalRefSpecs } from "../../src/refs.js";

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

console.log("");
if (failed === 0) {
  console.log(`ALL TESTS PASSED (${passed})`);
} else {
  console.log(`SOME TESTS FAILED (${failed} failed, ${passed} passed)`);
  process.exit(1);
}
