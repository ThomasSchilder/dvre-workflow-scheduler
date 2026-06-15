import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const ajv = new Ajv2020({ allErrors: true, strict: false });
addFormats(ajv);

const schemaPath = join(__dirname, "..", "schemas", "workflow-v1.json");
const schema = JSON.parse(readFileSync(schemaPath, "utf-8"));
const validate = ajv.compile(schema);

export function validateWorkflow(workflow) {
  const valid = validate(workflow);
  if (valid) {
    return { valid: true };
  }
  return {
    valid: false,
    errors: validate.errors?.map((e) => ({
      instancePath: e.instancePath,
      message: e.message,
      params: e.params,
    })),
  };
}
