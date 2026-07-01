import { resolveInfrastructure, resolveExternalRefSpecs, resolveWorkflowNodes } from "./refs.js";
import { resolveDag, computeServiceLifecycle } from "./dag.js";
import { generateId, now } from "./db.js";
import { buildTaskSpec, buildServiceSpec, buildVolumeSpec } from "./lib/spec.js";
import { toOperatorResourceName, toOperatorVolumeName, translateDependsOn } from "./lib/naming.js";
import { buildOperatorClients, getOperatorForBinding, getVolumeOperators } from "./lib/infra.js";
import { OperatorClient } from "./operator-client.js";

const SCHEDULER_HOST = process.env.SCHEDULER_HOST || "localhost";
const SCHEDULER_PORT = process.env.SCHEDULER_PORT || process.env.PORT || "3000";

function getWebhookUrl() {
  return `http://${SCHEDULER_HOST}:${SCHEDULER_PORT}/api/v1/webhooks/operator`;
}

async function deployTier(workflowId, tierNum, dag, config, externalRefSpecs,
                          serviceLifecycle, infraMap, operatorClients, db) {
  const { stmts } = db;
  const ts = now();
  const tierNodes = stmts.listNodesByWorkflowAndTier.all(workflowId, tierNum);
  console.log(`[scheduler] deployTier ${workflowId} tier=${tierNum} — ${tierNodes.length} node(s)`);

  for (const nodeRow of tierNodes) {
    const node = dag.nodes[`${nodeRow.section}.${nodeRow.name}`];
    if (!node) continue;

    const binding = node.binding || config.sections[node.section]?.binding;
    const client = binding ? getOperatorForBinding(binding, infraMap, operatorClients) : [...operatorClients.values()][0];

    if (node.type === "task") {
      if (nodeRow.operator_resource_id) continue;
      console.log(`[scheduler] ${workflowId} tier=${tierNum} — creating task "${nodeRow.section}.${nodeRow.name}" on operator`);
      const spec = buildTaskSpec(workflowId, nodeRow.id, node, config, externalRefSpecs);
      const result = await client.createTask(workflowId, spec);
      stmts.updateNodeOperatorIds.run(workflowId, result.id, ts, nodeRow.id);
      console.log(`[scheduler] ${workflowId} tier=${tierNum} — task "${nodeRow.section}.${nodeRow.name}" created: ${result.id}`);
    } else if (node.type === "service") {
      if (nodeRow.operator_resource_id) {
        if (nodeRow.desired_phase !== "Running") {
          console.log(`[scheduler] ${workflowId} tier=${tierNum} — starting existing service "${nodeRow.section}.${nodeRow.name}"`);
          await client.patchServiceDesiredPhase(workflowId, nodeRow.operator_resource_id, "Running");
          stmts.updateNodeDesiredPhase.run("Running", ts, nodeRow.id);
        }
        continue;
      }

      const lifecycle = serviceLifecycle[`${node.section}.${node.name}`];
      let desiredPhase = "Running";
      if (lifecycle) {
        if (lifecycle.noDependents) {
          desiredPhase = "Running";
        } else if (lifecycle.startBeforeTier === tierNum) {
          desiredPhase = "Running";
        } else if (lifecycle.startBeforeTier !== null && lifecycle.startBeforeTier > tierNum) {
          desiredPhase = "Stopped";
        }
      }

      console.log(`[scheduler] ${workflowId} tier=${tierNum} — creating service "${nodeRow.section}.${nodeRow.name}" (desiredPhase=${desiredPhase})`);
      const spec = buildServiceSpec(workflowId, nodeRow.id, node, config, externalRefSpecs, desiredPhase);
      const result = await client.createService(workflowId, spec);
      stmts.updateNodeOperatorIds.run(workflowId, result.id, ts, nodeRow.id);
      stmts.updateNodeDesiredPhase.run(desiredPhase, ts, nodeRow.id);
      console.log(`[scheduler] ${workflowId} tier=${tierNum} — service "${nodeRow.section}.${nodeRow.name}" created: ${result.id}`);
    }
  }
  console.log(`[scheduler] deployTier ${workflowId} tier=${tierNum} — complete`);
}

async function deployWorkflow(workflowId, db, authToken) {
  const { stmts } = db;
  const row = stmts.getWorkflow.get(workflowId);
  if (!row) {
    throw Object.assign(new Error("Workflow not found"), { status: 404 });
  }

  const config = JSON.parse(row.config_json);
  const dag = JSON.parse(row.dag_json);
  const serviceLifecycle = computeServiceLifecycle(dag);
  const ts = now();

  console.log(`[scheduler] deployWorkflow ${workflowId} — starting deployment`);

  let infraMap;
  try {
    infraMap = await resolveInfrastructure(config);
    console.log(`[scheduler] ${workflowId} — infrastructure resolved: ${[...infraMap.keys()].join(", ")}`);
  } catch (err) {
    console.error(`[scheduler] ${workflowId} — infrastructure resolution failed: ${err.message}`);
    stmts.updateWorkflowDeployError.run(err.message, ts, workflowId);
    throw Object.assign(new Error(`Infrastructure resolution failed: ${err.message}`), { status: 502 });
  }

  let externalRefSpecs;
  try {
    externalRefSpecs = await resolveExternalRefSpecs(config);
    console.log(`[scheduler] ${workflowId} — external refs resolved: ${Object.keys(externalRefSpecs).length} ref(s)`);
  } catch (err) {
    console.error(`[scheduler] ${workflowId} — external ref resolution failed: ${err.message}`);
    stmts.updateWorkflowDeployError.run(err.message, ts, workflowId);
    throw Object.assign(new Error(`External ref resolution failed: ${err.message}`), { status: 502 });
  }

  try {
    const resolvedSections = await resolveWorkflowNodes(config);
    config.sections = resolvedSections;
    console.log(`[scheduler] ${workflowId} — workflow nodes resolved`);
  } catch (err) {
    console.error(`[scheduler] ${workflowId} — task/service asset resolution failed: ${err.message}`);
    stmts.updateWorkflowDeployError.run(err.message, ts, workflowId);
    throw Object.assign(new Error(`Task/service asset resolution failed: ${err.message}`), { status: 502 });
  }

  const operatorClients = buildOperatorClients(infraMap, authToken);
  console.log(`[scheduler] ${workflowId} — operator clients built: ${[...operatorClients.keys()].join(", ")}`);

  stmts.updateWorkflowInfra.run(JSON.stringify([...infraMap.entries()].map(([k, v]) => [k, v])), ts, workflowId);
  stmts.updateWorkflowAuthToken.run(authToken, ts, workflowId);

  const deployedOperators = new Set();

  try {
    for (const [endpoint, client] of operatorClients) {
      console.log(`[scheduler] ${workflowId} — creating workflow on operator ${endpoint}`);
      await client.createWorkflow({ workflowName: workflowId });
      deployedOperators.add(endpoint);
    }

    for (const [endpoint, client] of operatorClients) {
      console.log(`[scheduler] ${workflowId} — registering webhook with operator ${endpoint}`);
      await client.registerWebhook({
        url: getWebhookUrl(),
        events: [],
      });
    }

    const volumes = config.volumes || {};
    const volCount = Object.keys(volumes).length;
    if (volCount > 0) {
      console.log(`[scheduler] ${workflowId} — creating ${volCount} volume(s)`);
    }
    for (const [volName, volDef] of Object.entries(volumes)) {
      const volSpec = buildVolumeSpec(workflowId, volName, volDef);
      const volOperators = getVolumeOperators(volName, config, infraMap, operatorClients);
      const operatorsToUse = volOperators.length > 0 ? volOperators : [...operatorClients.values()];

      let lastResourceId = null;
      for (const client of operatorsToUse) {
        const result = await client.createVolume(workflowId, volSpec);
        lastResourceId = result.id;
      }
      console.log(`[scheduler] ${workflowId} — volume "${volName}" created: ${lastResourceId}`);

      const volId = generateId("vol");
      stmts.insertVolume.run(
        volId, workflowId, volName, "Pending",
        lastResourceId, volDef.size,
        volDef.storageClass || null,
        volDef.accessMode || null,
        ts, ts
      );
    }

    console.log(`[scheduler] ${workflowId} — deploying tier 0`);
    await deployTier(workflowId, 0, dag, config, externalRefSpecs,
                     serviceLifecycle, infraMap, operatorClients, db);

    stmts.updateWorkflowStatus.run("Running", ts, workflowId);
    console.log(`[scheduler] ${workflowId} — deployment complete, status: Running`);
  } catch (err) {
    console.error(`[scheduler] ${workflowId} — deployment failed: ${err.message}`);
    for (const endpoint of deployedOperators) {
      const client = operatorClients.get(endpoint);
      try {
        await client.deleteWorkflow(workflowId);
      } catch (_) {}
    }
    stmts.updateWorkflowDeployError.run(err.message, ts, workflowId);
    throw Object.assign(new Error(`Deployment failed: ${err.message}`), { status: 502 });
  }
}

async function cancelWorkflow(workflowId, db) {
  const { stmts } = db;
  const row = stmts.getWorkflow.get(workflowId);
  if (!row) {
    throw Object.assign(new Error("Workflow not found"), { status: 404 });
  }

  const status = row.status;
  console.log(`[scheduler] cancelWorkflow ${workflowId} — current status: ${status}`);

  if (status === "Pending" || status === "Failed") {
    console.log(`[scheduler] ${workflowId} — terminal state, deleting records`);
    stmts.deleteEventsByWorkflow.run(workflowId);
    stmts.deleteVolumesByWorkflow.run(workflowId);
    stmts.deleteNodesByWorkflow.run(workflowId);
    stmts.deleteWorkflow.run(workflowId);
    return;
  }

  const ts = now();
  stmts.updateWorkflowStatus.run("Cancelling", ts, workflowId);

  if (row.infra_json) {
    try {
      const infraEntries = JSON.parse(row.infra_json);
      const endpoints = new Set();
      for (const [, v] of infraEntries) {
        if (v.endpoint) endpoints.add(v.endpoint);
      }
      for (const endpoint of endpoints) {
        console.log(`[scheduler] ${workflowId} — deleting workflow on operator ${endpoint}`);
        const client = new OperatorClient({ baseUrl: endpoint, authToken: row.auth_token });
        try {
          await client.deleteWorkflow(workflowId);
        } catch (err) {
          console.error(`[scheduler] ${workflowId} — operator ${endpoint} delete failed: ${err.message}`);
        }
      }
    } catch (err) {
      console.error(`[scheduler] ${workflowId} — failed to parse infra, skipping operator cleanup: ${err.message}`);
    }
  }

  stmts.deleteEventsByWorkflow.run(workflowId);
  stmts.deleteVolumesByWorkflow.run(workflowId);
  stmts.deleteNodesByWorkflow.run(workflowId);
  stmts.deleteWorkflow.run(workflowId);
  console.log(`[scheduler] ${workflowId} — cancelled and records deleted`);
}

export {
  deployWorkflow,
  cancelWorkflow,
  deployTier,
};
