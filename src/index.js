import express from "express";
import { initDb } from "./db.js";
import { healthRouter } from "./routes/health.js";
import { workflowsRouter } from "./routes/workflows.js";
import { eventsRouter } from "./routes/events.js";
import { webhooksRouter } from "./routes/webhooks.js";
import { visualizeRouter } from "./routes/visualize.js";
import { startPoller, stopPoller } from "./poller.js";

const PORT = parseInt(process.env.PORT || "3000", 10);
const DB_PATH = process.env.DB_PATH || "./data/scheduler.db";

async function main() {
  const db = initDb(DB_PATH);
  console.log(`[db] SQLite database initialized at ${DB_PATH}`);

  startPoller(db);

  const app = express();
  app.use(express.json({ limit: "10mb" }));

  app.use("/api/v1/health", healthRouter());
  app.use("/api/v1/workflows", workflowsRouter(db));
  app.use("/api/v1/workflows", eventsRouter(db));
  app.use("/api/v1/webhooks", webhooksRouter(db));
  app.use("/api/v1/visualize", visualizeRouter());

  app.listen(PORT, () => {
    console.log(`DVRE Workflow Scheduler listening on port ${PORT}`);
  });

  process.on("SIGTERM", () => {
    stopPoller();
    process.exit(0);
  });

  process.on("SIGINT", () => {
    stopPoller();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error starting scheduler:", err);
  process.exit(1);
});
