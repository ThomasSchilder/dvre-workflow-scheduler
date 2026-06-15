import { resolveInfrastructure, resolveExternalRefSpecs } from "./refs.js";
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

  for (const nodeRow of tierNodes) {
    const node = dag.nodes[`${nodeRow.section}.${nodeRow.name}`];
    if (!node) continue;

    const binding = node.binding || config.sections[node.section]?.binding;
    const client = binding ? getOperatorForBinding(binding, infraMap, operatorClients) : [...operatorClients.values()][0];

    if (node.type === "task") {
      if (nodeRow.operator_resource_id) continue;
      const spec = buildTaskSpec(workflowId, nodeRow.id, node, config, externalRefSpecs);
      const result = await client.createTask(workflowId, spec);
      stmts.updateNodeOperatorIds.run(workflowId, result.id, ts, nodeRow.id);
    } else if (node.type === "service") {
      if (nodeRow.operator_resource_id) {
        if (nodeRow.desired_phase !== "Running") {
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

      const spec = buildServiceSpec(workflowId, nodeRow.id, node, config, externalRefSpecs, desiredPhase);
      const result = await client.createService(workflowId, spec);
      stmts.updateNodeOperatorIds.run(workflowId, result.id, ts, nodeRow.id);
      stmts.updateNodeDesiredPhase.run(desiredPhase, ts, nodeRow.id);
    }
  }
}

async function deployWorkflow(workflowId, db) {
  const { stmts } = db;
  const row = stmts.getWorkflow.get(workflowId);
  if (!row) {
    throw Object.assign(new Error("Workflow not found"), { status: 404 });
  }

  const config = JSON.parse(row.config_json);
  const dag = JSON.parse(row.dag_json);
  const serviceLifecycle = computeServiceLifecycle(dag);
  const ts = now();

  let infraMap;
  try {
    infraMap = await resolveInfrastructure(config);
  } catch (err) {
    stmts.updateWorkflowDeployError.run(err.message, ts, workflowId);
    throw Object.assign(new Error(`Infrastructure resolution failed: ${err.message}`), { status: 502 });
  }

  let externalRefSpecs;
  try {
    externalRefSpecs = await resolveExternalRefSpecs(config);
  } catch (err) {
    stmts.updateWorkflowDeployError.run(err.message, ts, workflowId);
    throw Object.assign(new Error(`External ref resolution failed: ${err.message}`), { status: 502 });
  }

  const operatorClients = buildOperatorClients(infraMap);

  stmts.updateWorkflowInfra.run(JSON.stringify([...infraMap.entries()].map(([k, v]) => [k, v])), ts, workflowId);

  const deployedOperators = new Set();

  try {
    for (const [endpoint, client] of operatorClients) {
      await client.createWorkflow({ workflowName: workflowId });
      deployedOperators.add(endpoint);
    }

    for (const [endpoint, client] of operatorClients) {
      await client.registerWebhook({
        url: getWebhookUrl(),
        events: [],
      });
    }

    const volumes = config.volumes || {};
    for (const [volName, volDef] of Object.entries(volumes)) {
      const volSpec = buildVolumeSpec(workflowId, volName, volDef);
      const volOperators = getVolumeOperators(volName, config, infraMap, operatorClients);
      const operatorsToUse = volOperators.length > 0 ? volOperators : [...operatorClients.values()];

      let lastResourceId = null;
      for (const client of operatorsToUse) {
        const result = await client.createVolume(workflowId, volSpec);
        lastResourceId = result.id;
      }

      const volId = generateId("vol");
      stmts.insertVolume.run(
        volId, workflowId, volName, "Pending",
        lastResourceId, volDef.size,
        volDef.storageClass || null,
        volDef.accessMode || null,
        ts, ts
      );
    }

    await deployTier(workflowId, 0, dag, config, externalRefSpecs,
                     serviceLifecycle, infraMap, operatorClients, db);

    stmts.updateWorkflowStatus.run("Running", ts, workflowId);
  } catch (err) {
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

  if (status === "Pending" || status === "Failed") {
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
        const client = new OperatorClient({ baseUrl: endpoint });
        try {
          await client.deleteWorkflow(workflowId);
        } catch (_) {}
      }
    } catch (_) {}
  }

  stmts.deleteEventsByWorkflow.run(workflowId);
  stmts.deleteVolumesByWorkflow.run(workflowId);
  stmts.deleteNodesByWorkflow.run(workflowId);
  stmts.deleteWorkflow.run(workflowId);
}

export {
  deployWorkflow,
  cancelWorkflow,
  deployTier,
};
