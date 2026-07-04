import { existsSync, readdirSync, readFileSync } from "fs";
import { join } from "path";
import { now } from "./db.js";
import { broadcast } from "./routes/events.js";
import { toOperatorVolumeName } from "./lib/naming.js";
import { getOperatorClientForNode } from "./lib/infra.js";

const OUTPUTS_DIR = "./data/outputs";
const UPLOADS_DIR = "./data/uploads";
const FILE_WAIT_TIMEOUT_MS = 30000;
const FILE_POLL_INTERVAL_MS = 2000;

function findVolumeForPath(inputPath, volumeMounts) {
  let bestVolName = null;
  let bestMountPath = null;
  let bestLen = 0;

  for (const [volName, mountPath] of Object.entries(volumeMounts)) {
    if (
      inputPath === mountPath ||
      inputPath.startsWith(mountPath + "/")
    ) {
      if (mountPath.length > bestLen) {
        bestLen = mountPath.length;
        bestVolName = volName;
        bestMountPath = mountPath;
      }
    }
  }

  return { bestVolName, bestMountPath };
}

function waitForFile(filePath, timeoutMs, pollMs) {
  return new Promise((resolve, reject) => {
    const start = Date.now();

    function check() {
      if (existsSync(filePath)) {
        resolve(filePath);
        return;
      }
      if (Date.now() - start >= timeoutMs) {
        reject(new Error(`File not found after ${timeoutMs / 1000}s: ${filePath}`));
        return;
      }
      setTimeout(check, pollMs);
    }

    check();
  });
}

async function resolveInputContent(workflowId, inputKey, inputDef, config, dag) {
  const from = inputDef.from;

  if (from === "output") {
    const ref = inputDef.ref;
    if (!ref) {
      throw new Error(`Input "${inputKey}" has from=output but no ref`);
    }

    const parts = ref.split(".");
    if (parts.length !== 3) {
      throw new Error(`Input "${inputKey}" ref "${ref}" must be section.task.outputName`);
    }
    const [section, task, outputName] = parts;

    const taskDir = join(OUTPUTS_DIR, workflowId, `${section}.${task}`);
    const filePath = join(taskDir, outputName);

    if (!existsSync(filePath)) {
      throw new Error(`Output file not found for input "${inputKey}": ${filePath}`);
    }

    const content = readFileSync(filePath).toString("base64");
    return content;
  }

  if (from === "file") {
    const fileName = inputDef.fileName;
    if (!fileName) {
      throw new Error(`Input "${inputKey}" has from=file but no fileName`);
    }

    const dir = join(UPLOADS_DIR, workflowId, inputKey);
    const filePath = join(dir, fileName);

    console.log(`[inputs] ${workflowId} — waiting for file "${fileName}" at ${filePath}`);
    await waitForFile(filePath, FILE_WAIT_TIMEOUT_MS, FILE_POLL_INTERVAL_MS);
    console.log(`[inputs] ${workflowId} — file "${fileName}" arrived`);

    const content = readFileSync(filePath).toString("base64");
    return content;
  }

  throw new Error(`Input "${inputKey}" has unknown from="${from}"`);
}

async function pushTierInputs(workflowId, tierNum, dag, config, infraMap, operatorClients, db) {
  const { stmts } = db;
  const tierNodes = stmts.listNodesByWorkflowAndTier.all(workflowId, tierNum);

  for (const nodeRow of tierNodes) {
    if (nodeRow.type !== "task") continue;

    const sectionDef = config.sections?.[nodeRow.section] || {};
    const taskDef = sectionDef.tasks?.[nodeRow.name] || {};
    const inputs = taskDef.inputs;

    if (!inputs || Object.keys(inputs).length === 0) {
      continue;
    }

    console.log(`[inputs] ${workflowId} — pushing inputs for task "${nodeRow.section}.${nodeRow.name}" (tier ${tierNum})`);

    const sectionMounts = sectionDef.volumeMounts || {};
    const taskMounts = taskDef.volumeMounts || {};
    const volumeMounts = { ...sectionMounts, ...taskMounts };

    const node = dag.nodes[`${nodeRow.section}.${nodeRow.name}`];
    const client = getOperatorClientForNode(node, infraMap, operatorClients);

    if (!client) {
      throw new Error(`No operator client for task "${nodeRow.section}.${nodeRow.name}"`);
    }

    let pushed = 0;
    for (const [inputKey, inputDef] of Object.entries(inputs)) {
      if (!inputDef || !inputDef.path) {
        console.warn(`[inputs] ${workflowId} — input "${inputKey}" has no path, skipping`);
        continue;
      }

      const content = await resolveInputContent(workflowId, inputKey, inputDef, config, dag);

      const { bestVolName, bestMountPath } = findVolumeForPath(inputDef.path, volumeMounts);
      if (!bestVolName) {
        throw new Error(
          `Input "${inputKey}" path "${inputDef.path}" is not within any mounted volume for task "${nodeRow.section}.${nodeRow.name}"`
        );
      }

      const relativePath = inputDef.path.slice(bestMountPath.length).replace(/^\//, "");
      const operatorVolName = toOperatorVolumeName(workflowId, bestVolName);

      console.log(`[inputs] ${workflowId} — pushing file to volume "${operatorVolName}" at "${relativePath}"`);
      await client.pushVolumeFile(workflowId, operatorVolName, { relativePath, content });
      pushed++;
    }

    if (pushed > 0) {
      const ts = now();
      broadcast(workflowId, "task.inputs.pushed", {
        workflowId,
        nodeId: nodeRow.id,
        operatorResourceId: nodeRow.operator_resource_id || null,
        resourceType: "task",
        details: { section: nodeRow.section, task: nodeRow.name, count: pushed },
        timestamp: ts,
      });
    }
  }
}

export { pushTierInputs };
