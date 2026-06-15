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

const examplesDir = join(__dirname, "..", "..", "schemas", "examples");
const files = readdirSync(examplesDir).filter((f) => f.endsWith(".json"));

import { readdirSync } from "fs";

let passed = 0;
let failed = 0;

for (const file of files) {
  const workflow = JSON.parse(
    readFileSync(join(examplesDir, file), "utf-8")
  );
  const valid = validate(workflow);
  if (valid) {
    console.log(`PASS: ${file}`);
    passed++;
  } else {
    console.log(`FAIL: ${file}`);
    console.log(JSON.stringify(validate.errors, null, 2));
    failed++;
  }
}

const baseInfra = { a: { source: "direct", type: "kubernetes", endpoint: "http://localhost:8080" } };

function test(name, fn) {
  try {
    fn();
    console.log(`PASS: ${name}`);
    passed++;
  } catch (e) {
    console.log(`FAIL: ${name}: ${e.message}`);
    failed++;
  }
}

test("section-level volumeMounts", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    volumes: { data: { size: "1Gi" } },
    sections: { s: { volumes: ["data"], volumeMounts: { data: "/data" }, tasks: { t: { image: "a" } } } }
  });
  if (!valid) throw new Error(JSON.stringify(validate.errors));
});

test("ftp protocol in externalRef", () => {
  const valid = validate({
    apiVersion: "v1",
    metadata: { name: "x" },
    infrastructure: baseInfra,
    sections: { s: { tasks: { t: { image: "a" } } } },
    externalRefs: { r: { source: "direct", protocol: "ftp", uri: "ftp://example.com/data" } }
  });
  if (!valid) throw new Error(JSON.stringify(validate.errors));
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
