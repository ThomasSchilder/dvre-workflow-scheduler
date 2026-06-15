import { resolveDag, computeServiceLifecycle } from "../../src/dag.js";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { join, dirname } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const examplesDir = join(__dirname, "../../schemas/examples");

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

function assertThrows(fn, label) {
  try {
    fn();
    console.log(`  FAIL: ${label} (no error thrown)`);
    failed++;
  } catch (e) {
    console.log(`  PASS: ${label} (threw: ${e.message})`);
    passed++;
  }
}

function loadExample(name) {
  return JSON.parse(readFileSync(join(examplesDir, name), "utf-8"));
}

function section(opts = {}) {
  const section = {};
  if (opts.executionMode) section.executionMode = opts.executionMode;
  if (opts.dependsOn) section.dependsOn = opts.dependsOn;
  if (opts.binding) section.binding = opts.binding;
  if (opts.tasks) section.tasks = opts.tasks;
  if (opts.services) section.services = opts.services;
  return section;
}

function task(image, opts = {}) {
  const t = { image: image || "alpine:latest" };
  if (opts.dependsOn) t.dependsOn = opts.dependsOn;
  if (opts.binding) t.binding = opts.binding;
  return t;
}

function svc(image, opts = {}) {
  const s = { image: image || "nginx:alpine" };
  if (opts.dependsOn) s.dependsOn = opts.dependsOn;
  if (opts.port) s.port = opts.port;
  return s;
}

function minimalWorkflow(sections, infra) {
  return {
    apiVersion: "v1",
    metadata: { name: "test" },
    infrastructure: infra || {
      local: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" },
    },
    sections,
  };
}

console.log("\n=== Test 1: minimal.json ===");
{
  const wf = loadExample("minimal.json");
  const dag = resolveDag(wf);
  assert(dag.nodes["main.hello"], "node exists");
  assert(dag.nodes["main.hello"].type === "task", "type is task");
  assert(dag.nodes["main.hello"].tier === 0, "tier is 0");
  assert(dag.nodes["main.hello"].dependsOn.length === 0, "no dependsOn");
  assert(dag.tiers.length === 1, "1 tier");
  assert(dag.tiers[0].nodes.length === 1, "1 node in tier 0");
}

console.log("\n=== Test 2: climate-pipeline.json ===");
{
  const wf = loadExample("climate-pipeline.json");
  const dag = resolveDag(wf);

  assert(dag.nodes["ingestion.fetch-climate-data"], "fetch-climate-data exists");
  assert(dag.nodes["ingestion.fetch-climate-data"].tier === 0, "fetch-climate-data tier 0");

  assert(dag.nodes["ingestion.fetch-config"], "fetch-config exists");
  assert(dag.nodes["ingestion.fetch-config"].tier === 0, "fetch-config tier 0");

  assert(dag.nodes["processing.validate"], "validate exists");
  assert(dag.nodes["processing.validate"].tier === 1, "validate tier 1 (depends on ingestion)");
  assert(dag.nodes["processing.validate"].dependsOn.includes("ingestion.fetch-climate-data"), "validate depends on fetch-climate-data");

  assert(dag.nodes["processing.transform"], "transform exists");
  assert(dag.nodes["processing.transform"].tier === 2, "transform tier 2 (sequential after validate)");

  assert(dag.nodes["processing.api-service"], "api-service exists");
  assert(dag.nodes["processing.api-service"].type === "service", "api-service is service");
  assert(dag.nodes["processing.api-service"].dependsOn.includes("processing.transform"), "api-service depends on transform");

  assert(dag.nodes["analytics.report"], "report exists");
  assert(dag.nodes["analytics.report"].dependsOn.includes("processing.transform"), "report depends on processing.transform (qualified)");

  assert(dag.nodes["analytics.dashboard"], "dashboard exists");
  assert(dag.nodes["analytics.dashboard"].type === "service", "dashboard is service");

  const lifecycle = computeServiceLifecycle(dag);
  assert(lifecycle["processing.api-service"], "api-service lifecycle exists");
  assert(lifecycle["processing.api-service"].startBeforeTier != null, "api-service has startBeforeTier");
  assert(lifecycle["analytics.dashboard"].noDependents === true, "dashboard has no task dependents");
}

console.log("\n=== Test 3: sequential-with-services.json ===");
{
  const wf = loadExample("sequential-with-services.json");
  const dag = resolveDag(wf);

  assert(dag.nodes["setup.init-db"], "init-db exists");
  assert(dag.nodes["setup.seed-data"], "seed-data exists");
  assert(dag.nodes["setup.db-service"], "db-service exists");

  assert(dag.nodes["setup.seed-data"].dependsOn.includes("setup.init-db"), "seed-data depends on init-db (sequential)");
  assert(!dag.nodes["setup.db-service"].dependsOn.includes("setup.init-db"), "db-service NOT auto-chained (service)");

  assert(dag.nodes["compute.analyze-a"].tier >= dag.nodes["setup.init-db"].tier, "compute tasks after setup");
  assert(dag.nodes["compute.analyze-a"].dependsOn.length > 0, "compute.analyze-a has section-level deps");
}

console.log("\n=== Test 4: external-refs.json ===");
{
  const wf = loadExample("external-refs.json");
  const dag = resolveDag(wf);

  assert(dag.nodes["train.train-model"], "train-model exists");
  assert(dag.nodes["train.train-model"].tier === 0, "train-model tier 0");
  assert(dag.tiers.length === 1, "1 tier");
}

console.log("\n=== Test 5: asset-infra.json ===");
{
  const wf = loadExample("asset-infra.json");
  const dag = resolveDag(wf);

  assert(dag.nodes["preprocess.clean-data"], "clean-data exists");
  assert(dag.nodes["preprocess.clean-data"].tier === 0, "clean-data tier 0");

  assert(dag.nodes["training.train"], "train exists");
  assert(dag.nodes["training.train"].tier === 1, "train tier 1");
  assert(dag.nodes["training.train"].dependsOn.includes("preprocess.clean-data"), "train depends on clean-data (section-level)");
}

console.log("\n=== Test 6: Cycle detection ===");
{
  const wf = minimalWorkflow({
    a: section({ tasks: { x: task("alpine", { dependsOn: ["b.y"] }) } }),
    b: section({ tasks: { y: task("alpine", { dependsOn: ["a.x"] }) } }),
  });
  assertThrows(() => resolveDag(wf), "cycle detected between a.x and b.y");
}

console.log("\n=== Test 7: Invalid dependsOn ===");
{
  const wf = minimalWorkflow({
    main: section({ tasks: { x: task("alpine", { dependsOn: ["nonexistent"] }) } }),
  });
  assertThrows(() => resolveDag(wf), "invalid dependsOn target");
}

console.log("\n=== Test 8: Invalid section dependsOn ===");
{
  const wf = minimalWorkflow({
    a: section({ tasks: { x: task() } }),
    b: section({ dependsOn: ["nonexistent"], tasks: { y: task() } }),
  });
  assertThrows(() => resolveDag(wf), "invalid section dependsOn target");
}

console.log("\n=== Test 9: Service with no dependents ===");
{
  const wf = minimalWorkflow({
    main: section({
      tasks: { hello: task() },
      services: { web: svc("nginx", { port: 80 }) },
    }),
  });
  const dag = resolveDag(wf);
  const lifecycle = computeServiceLifecycle(dag);
  assert(lifecycle["main.web"].noDependents === true, "web has no task dependents");
  assert(lifecycle["main.web"].startBeforeTier === null, "startBeforeTier is null");
  assert(lifecycle["main.web"].stopAfterTier === null, "stopAfterTier is null");
}

console.log("\n=== Test 10: Service dependsOn service ===");
{
  const wf = minimalWorkflow({
    main: section({
      services: {
        db: svc("postgres", { port: 5432 }),
        api: svc("node", { port: 3000, dependsOn: ["db"] }),
      },
      tasks: {
        test: task("alpine", { dependsOn: ["api"] }),
      },
    }),
  });
  const dag = resolveDag(wf);
  assert(dag.nodes["main.api"].dependsOn.includes("main.db"), "api depends on db");
  assert(dag.nodes["main.test"].dependsOn.includes("main.api"), "test depends on api");
  assert(dag.nodes["main.db"].tier < dag.nodes["main.api"].tier, "db before api");
  assert(dag.nodes["main.api"].tier < dag.nodes["main.test"].tier, "api before test");

  const lifecycle = computeServiceLifecycle(dag);
  assert(lifecycle["main.db"].noDependents === false, "db has transitive task dependents");
  assert(lifecycle["main.api"].noDependents === false, "api has task dependents");
}

console.log("\n=== Test 11: Qualified cross-section dependsOn ===");
{
  const wf = minimalWorkflow({
    a: section({ tasks: { x: task() } }),
    b: section({
      tasks: {
        y: task("alpine", { dependsOn: ["a.x"] }),
      },
    }),
  });
  const dag = resolveDag(wf);
  assert(dag.nodes["b.y"].dependsOn.includes("a.x"), "b.y depends on a.x (qualified)");
  assert(dag.nodes["a.x"].tier === 0, "a.x tier 0");
  assert(dag.nodes["b.y"].tier === 1, "b.y tier 1");
}

console.log("\n=== Test 12: Sequential chaining order ===");
{
  const wf = minimalWorkflow({
    main: section({
      executionMode: "sequential",
      tasks: {
        first: task(),
        second: task(),
        third: task(),
      },
    }),
  });
  const dag = resolveDag(wf);
  assert(dag.nodes["main.first"].tier === 0, "first tier 0");
  assert(dag.nodes["main.second"].tier === 1, "second tier 1");
  assert(dag.nodes["main.third"].tier === 2, "third tier 2");
  assert(dag.nodes["main.second"].dependsOn.includes("main.first"), "second depends on first");
  assert(dag.nodes["main.third"].dependsOn.includes("main.second"), "third depends on second");
}

console.log("\n=== Test 13: Binding inheritance ===");
{
  const wf = minimalWorkflow({
    main: section({
      binding: "local",
      tasks: {
        a: task(),
        b: task("alpine", { binding: "gpu-cluster" }),
      },
    }),
  });
  const dag = resolveDag(wf);
  assert(dag.nodes["main.a"].binding === "local", "a inherits section binding");
  assert(dag.nodes["main.b"].binding === "gpu-cluster", "b overrides section binding");
}

console.log("\n=== Test 14: Service lifecycle - startBeforeTier ===");
{
  const wf = minimalWorkflow({
    setup: section({
      tasks: { init: task() },
      services: { db: svc("postgres", { port: 5432 }) },
    }),
    compute: section({
      dependsOn: ["setup"],
      tasks: { process: task("alpine", { dependsOn: ["setup.db"] }) },
    }),
  });
  const dag = resolveDag(wf);
  const lifecycle = computeServiceLifecycle(dag);
  assert(lifecycle["setup.db"].noDependents === false, "db has task dependents");
  assert(lifecycle["setup.db"].startBeforeTier === dag.nodes["compute.process"].tier, "db starts before process tier");
}

console.log("\n");
if (failed === 0) {
  console.log(`ALL ${passed} TESTS PASSED`);
} else {
  console.log(`${passed} passed, ${failed} failed`);
}
process.exit(failed);
