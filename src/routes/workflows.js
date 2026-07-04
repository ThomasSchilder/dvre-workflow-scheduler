import { Router } from "express";
import { existsSync } from "fs";
import { join } from "path";
import { validateWorkflow } from "../validator.js";
import { generateId, now, workflowToJSON, nodeToJSON, volumeToJSON, eventToJSON } from "../db.js";
import { resolveDag, computeServiceLifecycle } from "../dag.js";
import { deployWorkflow, cancelWorkflow } from "../scheduler.js";

const OUTPUTS_DIR = "./data/outputs";

function validateInputRefs(config, dag) {
  const errors = [];

  for (const [sectionName, section] of Object.entries(config.sections || {})) {
    if (!section || !section.tasks) continue;

    for (const [taskName, taskDef] of Object.entries(section.tasks)) {
      if (!taskDef || !taskDef.inputs) continue;

      const currentNode = dag.nodes[`${sectionName}.${taskName}`];
      if (!currentNode) continue;

      for (const [inputKey, inputDef] of Object.entries(taskDef.inputs)) {
        if (!inputDef || inputDef.from !== "output") continue;

        const ref = inputDef.ref;
        if (!ref) {
          errors.push(`Input "${inputKey}" on task "${sectionName}.${taskName}" has from=output but no ref`);
          continue;
        }

        const parts = ref.split(".");
        if (parts.length !== 3) {
          errors.push(`Input "${inputKey}" ref "${ref}" must be section.task.outputName`);
          continue;
        }

        const [refSection, refTask, refOutput] = parts;
        const refNode = dag.nodes[`${refSection}.${refTask}`];

        if (!refNode) {
          errors.push(`Input "${inputKey}" ref "${ref}" references unknown task`);
          continue;
        }

        if (refNode.tier >= currentNode.tier) {
          errors.push(
            `Input "${inputKey}" on task "${sectionName}.${taskName}" references task "${refSection}.${refTask}" which is not in a previous tier`
          );
          continue;
        }

        const refTaskDef = config.sections?.[refSection]?.tasks?.[refTask];
        const refOutputs = refTaskDef?.outputs;
        if (!refOutputs || !refOutputs[refOutput]) {
          errors.push(
            `Input "${inputKey}" ref "${ref}" references output "${refOutput}" which is not defined on task "${refSection}.${refTask}"`
          );
        }
      }
    }
  }

  return errors;
}

function workflowsRouter(db) {
  const { stmts } = db;
  const router = new Router({ mergeParams: true });

  router.post("/", async (req, res) => {
    const config = req.body;
    const authToken = req.headers.authorization?.startsWith("Bearer ")
      ? req.headers.authorization.slice(7)
      : null;

    if (!config || typeof config !== "object") {
      return res.status(400).json({ error: "Request body must be a JSON object" });
    }

    const wfName = config.metadata?.name || "(unnamed)";
    console.log(`[workflows] POST / — received workflow "${wfName}"`);

    const validation = validateWorkflow(config);
    if (!validation.valid) {
      console.log(`[workflows] "${wfName}" validation failed: ${validation.errors.length} error(s)`);
      return res.status(400).json({
        error: "Workflow validation failed",
        details: validation.errors,
      });
    }
    console.log(`[workflows] "${wfName}" schema validation passed`);

    let dag;
    try {
      dag = resolveDag(config);
    } catch (err) {
      console.log(`[workflows] "${wfName}" DAG resolution failed: ${err.message}`);
      return res.status(400).json({
        error: "DAG resolution failed",
        details: err.message,
      });
    }
    console.log(`[workflows] "${wfName}" DAG resolved: ${Object.keys(dag.nodes).length} node(s), ${dag.tiers.length} tier(s)`);

    const inputErrors = validateInputRefs(config, dag);
    if (inputErrors.length > 0) {
      console.log(`[workflows] "${wfName}" input ref validation failed: ${inputErrors.length} error(s)`);
      return res.status(400).json({
        error: "Input reference validation failed",
        details: inputErrors,
      });
    }

    const serviceLifecycle = computeServiceLifecycle(dag);

    const id = generateId("wf");
    const name = config.metadata?.name || id;
    const ts = now();

    stmts.insertWorkflow.run(id, name, "Created", JSON.stringify(config), null, null, null, ts, ts);
    if (authToken) {
      stmts.updateWorkflowAuthToken.run(authToken, ts, id);
    }
    console.log(`[workflows] "${wfName}" created as ${id} (status: Created)`);

    for (const [nodeId, node] of Object.entries(dag.nodes)) {
      const nodeIdPrefixed = `${id}.${nodeId}`;
      const lifecycle = serviceLifecycle[nodeId];
      let desiredPhase = null;
      if (node.type === "service" && lifecycle) {
        desiredPhase = lifecycle.noDependents ? "Running" : null;
      }

      stmts.insertNode.run(
        nodeIdPrefixed, id, node.type, node.section, node.name,
        "Pending", node.tier, JSON.stringify(node.dependsOn),
        node.binding, null, null, desiredPhase, ts, ts
      );
    }

    stmts.updateWorkflowDag.run(JSON.stringify(dag), "Created", ts, id);

    const workflow = workflowToJSON(stmts.getWorkflow.get(id));
    const nodeRows = stmts.listNodesByWorkflow.all(id);
    workflow.nodes = nodeRows.map(nodeToJSON);
    const volRows = stmts.listVolumesByWorkflow.all(id);
    workflow.volumes = volRows.map(volumeToJSON);
    res.status(201).json(workflow);
  });

  router.post("/:workflowId/deploy", async (req, res) => {
    const { workflowId } = req.params;
    const row = stmts.getWorkflow.get(workflowId);
    if (!row) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    if (row.status !== "Created") {
      return res.status(409).json({ error: `Workflow status is "${row.status}", must be "Created" to deploy` });
    }

    const ts = now();
    stmts.updateWorkflowStatus.run("Deploying", ts, workflowId);
    console.log(`[workflows] ${workflowId} — deploy requested, status: Deploying`);

    res.status(202).json({ id: workflowId, status: "Deploying" });

    deployWorkflow(workflowId, db).catch(err => {
      console.error(`[workflows] ${workflowId} — async deployment failed: ${err.message}`);
    });
  });

  router.get("/", (_req, res) => {
    const rows = stmts.listWorkflows.all();
    res.json({ data: rows.map(workflowToJSON) });
  });

  router.get("/:workflowId", (req, res) => {
    const { workflowId } = req.params;
    const row = stmts.getWorkflow.get(workflowId);
    if (!row) {
      return res.status(404).json({ error: "Workflow not found" });
    }
    const workflow = workflowToJSON(row);
    const nodeRows = stmts.listNodesByWorkflow.all(workflowId);
    workflow.nodes = nodeRows.map(nodeToJSON);
    const volRows = stmts.listVolumesByWorkflow.all(workflowId);
    workflow.volumes = volRows.map(volumeToJSON);
    res.json(workflow);
  });

  router.get("/:workflowId/events", (req, res) => {
    const { workflowId } = req.params;
    const row = stmts.getWorkflow.get(workflowId);
    if (!row) {
      return res.status(404).json({ error: "Workflow not found" });
    }
    const limit = req.query.limit ? parseInt(req.query.limit, 10) : null;
    let eventRows;
    if (limit && limit > 0) {
      eventRows = stmts.listEventsByWorkflowWithLimit.all(workflowId, limit).reverse();
    } else {
      eventRows = stmts.listEventsByWorkflow.all(workflowId);
    }
    res.json({ data: eventRows.map(eventToJSON) });
  });

  router.get("/:workflowId/outputs", (req, res) => {
    const { workflowId } = req.params;
    const row = stmts.getWorkflow.get(workflowId);
    if (!row) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    const zipPath = join(OUTPUTS_DIR, `${workflowId}.zip`);
    if (!existsSync(zipPath)) {
      return res.status(404).json({ error: "Outputs not yet available" });
    }

    res.download(zipPath, `${workflowId}-outputs.zip`);
  });

  router.delete("/:workflowId", async (req, res) => {
    const { workflowId } = req.params;
    const row = stmts.getWorkflow.get(workflowId);
    if (!row) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    console.log(`[workflows] DELETE ${workflowId} — current status: ${row.status}`);
    try {
      await cancelWorkflow(workflowId, db);
    } catch (err) {
      console.error(`[workflows] DELETE ${workflowId} — cancel failed: ${err.message}`);
      return res.status(err.status || 500).json({ error: err.message });
    }

    console.log(`[workflows] DELETE ${workflowId} — cancelled and deleted`);
    res.json({ id: workflowId, status: "Deleted" });
  });

  return router;
}

export { workflowsRouter };
