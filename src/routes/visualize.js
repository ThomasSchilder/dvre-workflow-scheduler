import { Router } from "express";
import { validateWorkflow } from "../validator.js";
import { resolveDag, computeServiceLifecycle } from "../dag.js";
import { toDot } from "../viz.js";
import { instance } from "@viz-js/viz";

let vizInstance = null;

async function getViz() {
  if (!vizInstance) {
    vizInstance = await instance();
  }
  return vizInstance;
}

function visualizeRouter() {
  const router = new Router();

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

    const lifecycle = computeServiceLifecycle(dag);
    const metadata = config.metadata || {};
    const dotOutput = toDot(dag, lifecycle, metadata, config);

    try {
      const viz = await getViz();
      const svg = viz.renderString(dotOutput, { format: "svg" });
      res.type("image/svg+xml").send(svg);
    } catch (err) {
      return res.status(500).json({
        error: "SVG rendering failed",
        details: err.message,
      });
    }
  });

  return router;
}

export { visualizeRouter };
