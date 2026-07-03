import { mkdirSync, existsSync, readdirSync, statSync } from "fs";
import { join } from "path";
import { now } from "./db.js";
import { broadcast } from "./routes/events.js";
import { buildOperatorClients, reconstructInfraMap, getOperatorClientForNode } from "./lib/infra.js";

const OUTPUTS_DIR = "./data/outputs";

async function collectTaskOutputs(workflowId, nodeId, db) {
  const { stmts } = db;

  const nodeRow = stmts.getNode.get(nodeId);
  if (!nodeRow) {
    console.warn(`[outputs] ${workflowId} — node ${nodeId} not found`);
    return;
  }

  const wfRow = stmts.getWorkflow.get(workflowId);
  if (!wfRow) {
    console.warn(`[outputs] ${workflowId} — workflow not found`);
    return;
  }

  const config = JSON.parse(wfRow.config_json);
  const sectionDef = config.sections?.[nodeRow.section] || {};
  const taskDef = sectionDef.tasks?.[nodeRow.name] || {};

  const outputs = taskDef.outputs;
  if (!outputs || Object.keys(outputs).length === 0) {
    return;
  }

  const infraMap = reconstructInfraMap(wfRow.infra_json);
  const operatorClients = buildOperatorClients(infraMap, wfRow.auth_token);
  const client = getOperatorClientForNode(nodeRow, infraMap, operatorClients);

  if (!client) {
    console.error(`[outputs] ${workflowId} — no operator client for node ${nodeRow.section}.${nodeRow.name}`);
    return;
  }

  const operatorResourceId = nodeRow.operator_resource_id;
  if (!operatorResourceId) {
    console.warn(`[outputs] ${workflowId} — node ${nodeRow.section}.${nodeRow.name} has no operator_resource_id`);
    return;
  }

  console.log(`[outputs] ${workflowId} — collecting outputs for ${nodeRow.section}.${nodeRow.name}`);

  let response;
  try {
    response = await client.getTaskOutputs(workflowId, operatorResourceId);
  } catch (err) {
    if (err.status === 404) {
      console.log(`[outputs] ${workflowId} — outputs not ready yet, retrying once...`);
      await new Promise((r) => setTimeout(r, 2000));
      try {
        response = await client.getTaskOutputs(workflowId, operatorResourceId);
      } catch (retryErr) {
        console.error(`[outputs] ${workflowId} — retry failed: ${retryErr.message}`);
        return;
      }
    } else {
      console.error(`[outputs] ${workflowId} — getTaskOutputs failed: ${err.message}`);
      return;
    }
  }

  const taskDir = join(OUTPUTS_DIR, workflowId, `${nodeRow.section}.${nodeRow.name}`);
  mkdirSync(taskDir, { recursive: true });

  let collected = 0;
  for (const [outputName, outputData] of Object.entries(response.outputs || {})) {
    if (outputData.error) {
      console.warn(`[outputs] ${workflowId} — output "${outputName}" error: ${outputData.error}`);
      continue;
    }
    if (!outputData.content) {
      console.warn(`[outputs] ${workflowId} — output "${outputName}" has no content`);
      continue;
    }

    const buf = Buffer.from(outputData.content, "base64");
    const filePath = join(taskDir, outputName);
    const { writeFileSync } = await import("fs");
    writeFileSync(filePath, buf);
    console.log(`[outputs] ${workflowId} — wrote ${outputName} (${buf.length} bytes) to ${filePath}`);
    collected++;
  }

  if (collected > 0) {
    const ts = now();
    broadcast(workflowId, "task.outputs.collected", {
      workflowId,
      nodeId,
      operatorResourceId,
      resourceType: "task",
      details: { section: nodeRow.section, task: nodeRow.name, count: collected },
      timestamp: ts,
    });
  }
}

async function createOutputZip(workflowId) {
  const taskDir = join(OUTPUTS_DIR, workflowId);
  const ts = now();

  if (!existsSync(taskDir)) {
    console.log(`[outputs] ${workflowId} — no outputs directory, skipping zip`);
    broadcast(workflowId, "workflow.outputs.ready", {
      workflowId,
      nodeId: null,
      operatorResourceId: null,
      resourceType: "workflow",
      details: { hasOutputs: false },
      timestamp: ts,
    });
    return;
  }

  const files = collectFilePaths(taskDir, "");
  if (files.length === 0) {
    console.log(`[outputs] ${workflowId} — no output files found`);
    broadcast(workflowId, "workflow.outputs.ready", {
      workflowId,
      nodeId: null,
      operatorResourceId: null,
      resourceType: "workflow",
      details: { hasOutputs: false },
      timestamp: ts,
    });
    return;
  }

  const filePaths = files.map(([rp]) => rp);

  broadcast(workflowId, "workflow.outputs.ready", {
    workflowId,
    nodeId: null,
    operatorResourceId: null,
    resourceType: "workflow",
    details: { hasOutputs: true, files: filePaths },
    timestamp: ts,
  });

  try {
    const archiver = (await import("archiver")).default;
    const { createWriteStream } = await import("fs");

    mkdirSync(OUTPUTS_DIR, { recursive: true });
    const zipPath = join(OUTPUTS_DIR, `${workflowId}.zip`);
    const output = createWriteStream(zipPath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    archive.pipe(output);

    for (const [relativePath, fullPath] of files) {
      archive.file(fullPath, { name: relativePath });
    }

    await archive.finalize();
    console.log(`[outputs] ${workflowId} — created zip with ${files.length} file(s) at ${zipPath}`);
  } catch (err) {
    console.error(`[outputs] ${workflowId} — createOutputZip failed: ${err.message}`);
  }
}

function collectFilePaths(baseDir, prefix) {
  const results = [];
  if (!existsSync(baseDir)) {
    return results;
  }

  const entries = readdirSync(baseDir);
  for (const entry of entries) {
    const fullPath = join(baseDir, entry);
    const relativePath = prefix ? `${prefix}/${entry}` : entry;
    const stat = statSync(fullPath);
    if (stat.isDirectory()) {
      results.push(...collectFilePaths(fullPath, relativePath));
    } else {
      results.push([relativePath, fullPath]);
    }
  }

  return results;
}

export { collectTaskOutputs, createOutputZip };
