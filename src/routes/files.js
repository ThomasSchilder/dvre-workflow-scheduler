import { Router } from "express";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { now } from "../db.js";

const UPLOADS_DIR = "./data/uploads";

function filesRouter(db) {
  const router = new Router({ mergeParams: true });

  router.post("/:workflowId/files", async (req, res) => {
    const { workflowId } = req.params;
    const { key, name, content } = req.body;

    if (!key || !name || !content) {
      return res.status(400).json({ error: "Missing key, name, or content" });
    }

    const row = db.stmts.getWorkflow.get(workflowId);
    if (!row) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    try {
      const dir = join(UPLOADS_DIR, workflowId, key);
      mkdirSync(dir, { recursive: true });

      const buf = Buffer.from(content, "base64");
      const filePath = join(dir, name);
      writeFileSync(filePath, buf);

      console.log(`[files] ${workflowId} — stored file "${name}" (${buf.length} bytes) under key "${key}"`);
      res.status(201).json({ key, name, size: buf.length });
    } catch (err) {
      console.error(`[files] ${workflowId} — upload failed: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}

export { filesRouter };
