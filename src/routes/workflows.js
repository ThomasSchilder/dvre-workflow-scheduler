import { Router } from "express";
import { validateWorkflow } from "../validator.js";
import { generateId, now, workflowToJSON, nodeToJSON, volumeToJSON, eventToJSON } from "../db.js";
import { resolveDag, computeServiceLifecycle } from "../dag.js";
import { deployWorkflow, cancelWorkflow } from "../scheduler.js";

function workflowsRouter(db) {
  const { stmts } = db;
  const router = new Router({ mergeParams: true });

  router.post("/", async (req, res) => {
    const config = req.body;

    if (!config || typeof config !== "object") {
      return res.status(400).json({ error: "Request body must be a JSON object" });
    }

    const validation = validateWorkflow(config);
    if (!validation.valid) {
      return res.status(400).json({
        error: "Workflow validation failed",
        details: validation.errors,
      });
    }

    let dag;
    try {
      dag = resolveDag(config);
    } catch (err) {
      return res.status(400).json({
        error: "DAG resolution failed",
        details: err.message,
      });
    }

    const serviceLifecycle = computeServiceLifecycle(dag);

    const id = generateId("wf");
    const name = config.metadata?.name || id;
    const ts = now();

    stmts.insertWorkflow.run(id, name, "Deploying", JSON.stringify(config), null, null, null, ts, ts);

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

    try {
      await deployWorkflow(id, db);
    } catch (err) {
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

  router.delete("/:workflowId", async (req, res) => {
    const { workflowId } = req.params;
    const row = stmts.getWorkflow.get(workflowId);
    if (!row) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    try {
      await cancelWorkflow(workflowId, db);
    } catch (err) {
      return res.status(err.status || 500).json({ error: err.message });
    }

    res.json({ id: workflowId, status: "Deleted" });
  });

  return router;
}

export { workflowsRouter };
