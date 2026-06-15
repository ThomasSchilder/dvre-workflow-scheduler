import { Router } from "express";

const sseClients = new Map();

function addClient(workflowId, res) {
  if (!sseClients.has(workflowId)) {
    sseClients.set(workflowId, new Set());
  }
  sseClients.get(workflowId).add(res);
}

function removeClient(workflowId, res) {
  const clients = sseClients.get(workflowId);
  if (clients) {
    clients.delete(res);
    if (clients.size === 0) {
      sseClients.delete(workflowId);
    }
  }
}

function broadcast(workflowId, event, data) {
  const clients = sseClients.get(workflowId);
  if (!clients) return;
  const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of clients) {
    try {
      res.write(message);
    } catch (_) {
      removeClient(workflowId, res);
    }
  }
}

function eventsRouter(db) {
  const router = new Router({ mergeParams: true });

  router.get("/:workflowId/events", (req, res) => {
    const { workflowId } = req.params;
    const row = db.stmts.getWorkflow.get(workflowId);
    if (!row) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });

    res.write(`event: connected\ndata: ${JSON.stringify({ workflowId })}\n\n`);

    addClient(workflowId, res);

    const heartbeat = setInterval(() => {
      try {
        res.write(": heartbeat\n\n");
      } catch (_) {
        clearInterval(heartbeat);
        removeClient(workflowId, res);
      }
    }, 30000);

    req.on("close", () => {
      clearInterval(heartbeat);
      removeClient(workflowId, res);
    });
  });

  return router;
}

export { eventsRouter, broadcast };
