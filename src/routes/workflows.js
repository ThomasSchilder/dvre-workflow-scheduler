import { Router } from "express";
import { existsSync } from "fs";
import { join } from "path";
import { validateWorkflow } from "../validator.js";
import { generateId, now, workflowToJSON, nodeToJSON, volumeToJSON, eventToJSON } from "../db.js";
import { resolveDag, computeServiceLifecycle } from "../dag.js";
import { deployWorkflow, cancelWorkflow } from "../scheduler.js";

const OUTPUTS_DIR = "./data/outputs";

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
    console.log(`[workflows] "${wfName}" validation passed`);

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

    const serviceLifecycle = computeServiceLifecycle(dag);

    const id = generateId("wf");
    const name = config.metadata?.name || id;
    const ts = now();

    stmts.insertWorkflow.run(id, name, "Deploying", JSON.stringify(config), null, null, null, ts, ts);
    console.log(`[workflows] "${wfName}" created as ${id}`);

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

    stmts.updateWorkflowDag.run(JSON.stringify(dag), "Deploying", ts, id);

    console.log(`[workflows] ${id} — deploying (authToken: ${authToken ? "yes" : "no"})`);
    try {
      await deployWorkflow(id, db, authToken);
    } catch (err) {
      console.error(`[workflows] ${id} — deployment failed: ${err.message}`);
      const workflow = workflowToJSON(stmts.getWorkflow.get(id));
      const nodeRows = stmts.listNodesByWorkflow.all(id);
      workflow.nodes = nodeRows.map(nodeToJSON);
      const volRows = stmts.listVolumesByWorkflow.all(id);
      workflow.volumes = volRows.map(volumeToJSON);
      return res.status(err.status || 502).json({
        error: err.message,
        workflow,
      });
    }

    console.log(`[workflows] ${id} — deployed successfully, status: Running`);
    const workflow = workflowToJSON(stmts.getWorkflow.get(id));
    const nodeRows = stmts.listNodesByWorkflow.all(id);
    workflow.nodes = nodeRows.map(nodeToJSON);
    const volRows = stmts.listVolumesByWorkflow.all(id);
    workflow.volumes = volRows.map(volumeToJSON);
    res.status(201).json(workflow);
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
