import { now } from "./db.js";
import { computeServiceLifecycle } from "./dag.js";
import { resolveExternalRefSpecs, resolveWorkflowNodes } from "./refs.js";
import { deployTier } from "./scheduler.js";
import { buildOperatorClients, reconstructInfraMap, getOperatorClientForNode } from "./lib/infra.js";
import { OperatorClient } from "./operator-client.js";
import { broadcast } from "./routes/events.js";
import { collectTaskOutputs, createOutputZip } from "./outputs.js";
import { pushTierInputs } from "./inputs.js";
import {
  isTierComplete,
  getHighestCompletedTier,
  hasTaskFailed,
  getServicesToStart,
  getServicesToStop,
} from "./lifecycle.js";

const advancingWorkflows = new Set();

const EVENT_TO_PHASE = {
  "task.succeeded": "Succeeded",
  "task.failed": "Failed",
  "task.running": "Running",
  "service.running": "Running",
  "service.failed": "Failed",
  "service.stopped": "Stopped",
};

const VOLUME_EVENT_TO_PHASE = {
  "volume.bound": "Bound",
  "volume.failed": "Failed",
};

async function handleWebhook(db, event, workflowId, resourceId, resourceType, details, timestamp) {
  const { stmts } = db;
  const ts = timestamp || now();

  const schedulerNodeId = resolveSchedulerNodeId(stmts, resourceId, resourceType);

  console.log(`[tracker] ${workflowId} — event: ${event} (resource=${resourceType}:${resourceId || "none"})`);

  stmts.insertEvent.run(workflowId, schedulerNodeId, event, JSON.stringify(details || {}), ts);

  if (schedulerNodeId) {
    const phase = EVENT_TO_PHASE[event];
    if (phase) {
      stmts.updateNodeStatus.run(phase, ts, schedulerNodeId);
    }
  }

  if (resourceType === "volume" && resourceId) {
    const volRow = stmts.getVolumeByOperatorResourceId.get(resourceId);
    if (volRow) {
      const volPhase = VOLUME_EVENT_TO_PHASE[event];
      if (volPhase) {
        stmts.updateVolumeStatus.run(volPhase, ts, volRow.id);
      }
    }
  }

  if (event === "workflow.failed") {
    await handleFailure(workflowId, db);
    return;
  }

  if (event === "workflow.completed") {
    await maybeAdvanceTier(workflowId, db, null);
    return;
  }

  broadcast(workflowId, event, {
    workflowId,
    nodeId: schedulerNodeId,
    operatorResourceId: resourceId || null,
    resourceType: resourceType || null,
    details: details || {},
    timestamp: ts,
  });

  if (event === "task.failed" || event === "service.failed") {
    console.log(`[tracker] ${workflowId} — ${event}, handling failure`);
    await handleFailure(workflowId, db);
  } else if (event === "task.succeeded") {
    console.log(`[tracker] ${workflowId} — task succeeded, collecting outputs then checking tier advancement`);
    await collectTaskOutputs(workflowId, schedulerNodeId, db);
    await maybeAdvanceTier(workflowId, db, schedulerNodeId);
  }
}

function resolveSchedulerNodeId(stmts, resourceId, resourceType) {
  if (!resourceId) return null;
  if (resourceType === "workflow") return null;
  if (resourceType === "volume") return null;

  const nodeRow = stmts.getNodeByOperatorResourceId.get(resourceId);
  if (nodeRow) return nodeRow.id;

  return null;
}

async function handleFailure(workflowId, db) {
  const { stmts } = db;
  const row = stmts.getWorkflow.get(workflowId);
  if (!row) return;

  const terminalStates = new Set(["Failed", "Succeeded", "Cancelling", "Deleted"]);
  if (terminalStates.has(row.status)) return;

  console.error(`[tracker] ${workflowId} — failing workflow (was ${row.status})`);
  const ts = now();
  stmts.updateWorkflowStatus.run("Failed", ts, workflowId);

  broadcast(workflowId, "workflow.failed", {
    workflowId,
    nodeId: null,
    operatorResourceId: null,
    resourceType: "workflow",
    details: { phase: "Failed" },
    timestamp: ts,
  });

  await stopAllRunningServices(workflowId, db);
  await createOutputZip(workflowId);
}

async function maybeAdvanceTier(workflowId, db, schedulerNodeId) {
  const { stmts } = db;
  const row = stmts.getWorkflow.get(workflowId);
  if (!row) return;

  const terminalStates = new Set(["Failed", "Succeeded", "Cancelling", "Deleted"]);
  if (terminalStates.has(row.status)) return;

  if (advancingWorkflows.has(workflowId)) return;

  const allNodes = stmts.listNodesByWorkflow.all(workflowId);
  const dag = JSON.parse(row.dag_json);
  const tiers = dag.tiers;

  if (schedulerNodeId) {
    const nodeRow = stmts.getNode.get(schedulerNodeId);
    if (!nodeRow) return;
    if (!isTierComplete(nodeRow.tier, allNodes, tiers)) {
      console.log(`[tracker] ${workflowId} — tier ${nodeRow.tier} not yet complete`);
      return;
    }
    console.log(`[tracker] ${workflowId} — tier ${nodeRow.tier} complete, advancing`);
  } else {
    if (getHighestCompletedTier(allNodes, tiers) < 0) return;
  }

  await advanceTier(workflowId, db);
}

async function advanceTier(workflowId, db) {
  advancingWorkflows.add(workflowId);

  try {
    const { stmts } = db;
    const row = stmts.getWorkflow.get(workflowId);
    if (!row) return;

    const config = JSON.parse(row.config_json);
    const dag = JSON.parse(row.dag_json);
    const serviceLifecycle = computeServiceLifecycle(dag);
    const tiers = dag.tiers;

    const allNodes = stmts.listNodesByWorkflow.all(workflowId);
    const highestCompletedTier = getHighestCompletedTier(allNodes, tiers);
    const nextTier = highestCompletedTier + 1;

    if (nextTier > tiers[tiers.length - 1].tier) {
      console.log(`[tracker] ${workflowId} — all tiers complete, workflow succeeded`);
      await stopAllRunningServices(workflowId, db);

      const ts = now();
      stmts.updateWorkflowStatus.run("Succeeded", ts, workflowId);

      await createOutputZip(workflowId);

      broadcast(workflowId, "workflow.succeeded", {
        workflowId,
        nodeId: null,
        operatorResourceId: null,
        resourceType: "workflow",
        details: { phase: "Succeeded" },
        timestamp: ts,
      });

      return;
    }

    console.log(`[tracker] ${workflowId} — advancing to tier ${nextTier}`);

    const infraMap = reconstructInfraMap(row.infra_json);
    const operatorClients = buildOperatorClients(infraMap, row.auth_token);

    const servicesToStart = getServicesToStart(nextTier, allNodes, serviceLifecycle);
    const ts = now();
    for (const svcNode of servicesToStart) {
      if (svcNode.operator_resource_id) {
        console.log(`[tracker] ${workflowId} — starting service "${svcNode.section}.${svcNode.name}" for tier ${nextTier}`);
        const client = getOperatorClientForNode(svcNode, infraMap, operatorClients);
        await client.patchServiceDesiredPhase(workflowId, svcNode.operator_resource_id, "Running");
        stmts.updateNodeDesiredPhase.run("Running", ts, svcNode.id);
      }
    }

    let externalRefSpecs;
    try {
      externalRefSpecs = await resolveExternalRefSpecs(config);
    } catch (err) {
      console.error(`[tracker] ${workflowId} — external ref resolution failed: ${err.message}`);
      const failTs = now();
      stmts.updateWorkflowStatus.run("Failed", failTs, workflowId);
      broadcast(workflowId, "workflow.failed", {
        workflowId, nodeId: null, operatorResourceId: null, resourceType: "workflow",
        details: { phase: "Failed", error: err.message }, timestamp: failTs,
      });
      return;
    }

    try {
      const resolvedSections = await resolveWorkflowNodes(config);
      config.sections = resolvedSections;
    } catch (err) {
      console.error(`[tracker] ${workflowId} — workflow node resolution failed: ${err.message}`);
      const failTs = now();
      stmts.updateWorkflowStatus.run("Failed", failTs, workflowId);
      broadcast(workflowId, "workflow.failed", {
        workflowId, nodeId: null, operatorResourceId: null, resourceType: "workflow",
        details: { phase: "Failed", error: err.message }, timestamp: failTs,
      });
      return;
    }

    console.log(`[tracker] ${workflowId} — pushing inputs for tier ${nextTier}`);
    await pushTierInputs(workflowId, nextTier, dag, config, infraMap, operatorClients, db);

    console.log(`[tracker] ${workflowId} — deploying tier ${nextTier}`);
    await deployTier(workflowId, nextTier, dag, config, externalRefSpecs,
                     serviceLifecycle, infraMap, operatorClients, db);

    const updatedNodes = stmts.listNodesByWorkflow.all(workflowId);
    const servicesToStop = getServicesToStop(nextTier, updatedNodes, serviceLifecycle);
    const stopTs = now();
    for (const svcNode of servicesToStop) {
      if (svcNode.operator_resource_id) {
        console.log(`[tracker] ${workflowId} — stopping service "${svcNode.section}.${svcNode.name}" after tier ${nextTier}`);
        const client = getOperatorClientForNode(svcNode, infraMap, operatorClients);
        await client.patchServiceDesiredPhase(workflowId, svcNode.operator_resource_id, "Stopped");
        stmts.updateNodeDesiredPhase.run("Stopped", stopTs, svcNode.id);
      }
    }

    console.log(`[tracker] ${workflowId} — tier ${nextTier} deployed`);
    broadcast(workflowId, "tier.advanced", {
      workflowId,
      nodeId: null,
      operatorResourceId: null,
      resourceType: "workflow",
      details: { tier: nextTier },
      timestamp: now(),
    });
  } catch (err) {
    console.error(`[tracker] ${workflowId} — tier advancement failed: ${err.message}`);
    const { stmts } = db;
    const failTs = now();
    stmts.updateWorkflowStatus.run("Failed", failTs, workflowId);
    stmts.updateWorkflowDeployError.run(err.message, failTs, workflowId);
    broadcast(workflowId, "workflow.failed", {
      workflowId, nodeId: null, operatorResourceId: null, resourceType: "workflow",
      details: { phase: "Failed", error: err.message }, timestamp: failTs,
    });
  } finally {
    advancingWorkflows.delete(workflowId);
  }
}

async function stopAllRunningServices(workflowId, db) {
  const { stmts } = db;
  const row = stmts.getWorkflow.get(workflowId);
  if (!row || !row.infra_json) return;

  const infraMap = reconstructInfraMap(row.infra_json);
  const operatorClients = buildOperatorClients(infraMap, row.auth_token);

  const allNodes = stmts.listNodesByWorkflow.all(workflowId);
  const ts = now();

  let stopped = 0;
  for (const node of allNodes) {
    if (node.type !== "service") continue;
    if (!node.operator_resource_id) continue;
    if (node.desired_phase === "Stopped") continue;

    console.log(`[tracker] ${workflowId} — stopping service "${node.section}.${node.name}"`);
    const client = getOperatorClientForNode(node, infraMap, operatorClients);
    try {
      await client.patchServiceDesiredPhase(workflowId, node.operator_resource_id, "Stopped");
      stmts.updateNodeDesiredPhase.run("Stopped", ts, node.id);
      stopped++;
    } catch (err) {
      console.error(`[tracker] ${workflowId} — failed to stop service "${node.section}.${node.name}": ${err.message}`);
    }
  }
  if (stopped > 0) {
    console.log(`[tracker] ${workflowId} — stopped ${stopped} service(s)`);
  }
}

export { handleWebhook, maybeAdvanceTier, advanceTier, handleFailure, stopAllRunningServices, advancingWorkflows };
