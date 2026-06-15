import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const ajv = new Ajv2020({ allErrors: true, strict: false });
addFormats(ajv);

const schemaPath = join(__dirname, "..", "..", "schemas", "workflow-v1.json");
const schema = JSON.parse(readFileSync(schemaPath, "utf-8"));
const validate = ajv.compile(schema);

let testsPassed = 0;
let testsFailed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`PASS: ${name}`);
    testsPassed++;
  } catch (e) {
    console.log(`FAIL: ${name}: ${e.message}`);
    testsFailed++;
  }
}

const baseInfra = { a: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" } };

test("missing apiVersion", () => {
  const valid = validate({ metadata: { name: "x" }, infrastructure: baseInfra, sections: {} });
  if (valid) throw new Error("should be invalid");
});

test("missing infrastructure", () => {
  const valid = validate({ apiVersion: "v1", metadata: { name: "x" }, sections: {} });
  if (valid) throw new Error("should be invalid");
});

test("invalid apiVersion", () => {
  const valid = validate({ apiVersion: "v2", metadata: { name: "x" }, infrastructure: baseInfra, sections: { s: { tasks: {} } } });
  if (valid) throw new Error("should be invalid");
});

test("task missing image", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { tasks: { t: { command: ["echo"] } } } }
  });
  if (valid) throw new Error("should be invalid");
});

test("infra direct missing endpoint", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: { a: { source: "direct", type: "kubernetes" } },
    sections: { s: { tasks: { t: { image: "a" } } } }
  });
  if (valid) throw new Error("should be invalid");
});

test("infra asset without assetId", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: { a: { source: "asset" } },
    sections: { s: { tasks: { t: { image: "a" } } } }
  });
  if (valid) throw new Error("should be invalid");
});

test("infra direct missing type", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: { a: { source: "direct", endpoint: "http://localhost:8080" } },
    sections: { s: { tasks: { t: { image: "a" } } } }
  });
  if (valid) throw new Error("should be invalid");
});

test("externalRef asset without assetId", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { tasks: { t: { image: "a" } } } },
    externalRefs: { r: { source: "asset" } }
  });
  if (valid) throw new Error("should be invalid");
});

test("externalRef direct without uri", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { tasks: { t: { image: "a" } } } },
    externalRefs: { r: { source: "direct", protocol: "s3" } }
  });
  if (valid) throw new Error("should be invalid");
});

test("invalid executionMode", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { executionMode: "mixed", tasks: { t: { image: "a" } } } }
  });
  if (valid) throw new Error("should be invalid");
});

test("service without image", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { services: { svc: { port: 8080 } } } }
  });
  if (valid) throw new Error("should be invalid");
});

test("port out of range", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { services: { svc: { image: "a", port: 99999 } } } }
  });
  if (valid) throw new Error("should be invalid");
});

test("additional property rejected", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { tasks: { t: { image: "a", unknownField: "x" } } } }
  });
  if (valid) throw new Error("should be invalid");
});

test("invalid externalRef protocol", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { tasks: { t: { image: "a" } } } },
    externalRefs: { r: { source: "direct", protocol: "websocket", uri: "ws://example.com" } }
  });
  if (valid) throw new Error("should be invalid");
});

console.log(`\n${testsPassed} passed, ${testsFailed} failed`);
process.exit(testsFailed > 0 ? 1 : 0);
