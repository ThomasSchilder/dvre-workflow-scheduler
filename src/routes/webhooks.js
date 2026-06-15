import { Router } from "express";
import { handleWebhook } from "../tracker.js";

function webhooksRouter(db) {
  const router = new Router();

  router.post("/operator", async (req, res) => {
    const { event, workflowId, resourceId, resourceType, timestamp, details } = req.body;

    if (!event || !workflowId) {
      return res.status(400).json({ error: "Missing event or workflowId" });
    }

    await handleWebhook(db, event, workflowId, resourceId, resourceType, details, timestamp);

    res.json({ ok: true });
  });

  return router;
}

export { webhooksRouter };
