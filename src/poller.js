import { now } from "./db.js";
import { buildOperatorClients, reconstructInfraMap } from "./lib/infra.js";
import { handleWebhook, stopAllRunningServices } from "./tracker.js";
import { broadcast } from "./routes/events.js";

const DEFAULT_POLL_INTERVAL_MS = 30000;
const DEFAULT_POLL_MAX_RETRIES = 3;
const DEFAULT_WORKFLOW_TIMEOUT_MS = 0;

function getPollInterval() {
  return parseInt(process.env.POLL_INTERVAL_MS || String(DEFAULT_POLL_INTERVAL_MS), 10);
}

function getPollMaxRetries() {
  return parseInt(process.env.POLL_MAX_RETRIES || String(DEFAULT_POLL_MAX_RETRIES), 10);
}

function getWorkflowTimeout() {
  return parseInt(process.env.WORKFLOW_TIMEOUT_MS || String(DEFAULT_WORKFLOW_TIMEOUT_MS), 10);
}

const OPERATOR_TASK_PHASE_MAP = {
  Pending: "Pending",
  Running: "Running",
  Succeeded: "Succeeded",
  Failed: "Failed",
};

const OPERATOR_SERVICE_PHASE_MAP = {
  Pending: "Pending",
  Running: "Running",
  Stopped: "Stopped",
  Failed: "Failed",
};

const OPERATOR_VOLUME_PHASE_MAP = {
  Pending: "Pending",
  Bound: "Bound",
  Failed: "Failed",
};

let pollTimer = null;

function startPoller(db) {
  reconcileAll(db).catch((err) => {
    console.error("[poller] startup reconciliation failed:", err.message);
  });

  const interval = getPollInterval();
  pollTimer = setInterval(() => {
    reconcileAll(db).catch((err) => {
      console.error("[poller] reconciliation error:", err.message);
    });
  }, interval);

  console.log(`[poller] started (interval=${interval}ms, maxRetries=${getPollMaxRetries()}, timeout=${getWorkflowTimeout()}ms)`);
}

function stopPoller() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
    console.log("[poller] stopped");
  }
}

async function reconcileAll(db) {
  const { stmts } = db;
  const runningWorkflows = stmts.getRunningWorkflows.all();

  if (runningWorkflows.length > 0) {
    console.log(`[poller] reconciling ${runningWorkflows.length} running workflow(s)`);
  }

  for (const wf of runningWorkflows) {
    try {
      await reconcileWorkflow(wf, db);
    } catch (err) {
      console.error(`[poller] error reconciling workflow ${wf.id}:`, err.message);
    }
    try {
      await checkWorkflowTimeout(wf, db);
    } catch (err) {
      console.error(`[poller] error checking timeout for workflow ${wf.id}:`, err.message);
    }
  }
}

async function reconcileWorkflow(wf, db) {
  const { stmts } = db;

  const terminalStates = new Set(["Failed", "Succeeded", "Cancelling", "Deleted"]);
  if (terminalStates.has(wf.status)) return;

  if (!wf.infra_json) return;

  let infraMap;
  try {
    infraMap = reconstructInfraMap(wf.infra_json);
  } catch (_) {
    return;
  }

  const operatorClients = buildOperatorClients(infraMap, wf.auth_token);
  const maxRetries = getPollMaxRetries();

  let anySuccess = false;
  let workflowNotFound = false;
  let authError = false;

  for (const [endpoint, client] of operatorClients) {
    try {
      const [taskList, serviceList, volumeList] = await Promise.all([
        client.listTasks(wf.id).catch((err) => {
          if (err.status === 404) {
            workflowNotFound = true;
          } else if (err.status === 401) {
            authError = true;
            console.error(`[poller] ${wf.id} — auth error from operator ${endpoint}: ${err.message}`);
          }
          return null;
        }),
        client.listServices(wf.id).catch((err) => {
          if (err.status === 404) {
            workflowNotFound = true;
          } else if (err.status === 401) {
            authError = true;
            console.error(`[poller] ${wf.id} — auth error from operator ${endpoint}: ${err.message}`);
          }
          return null;
        }),
        client.listVolumes(wf.id).catch((err) => {
          if (err.status === 404) {
            workflowNotFound = true;
          } else if (err.status === 401) {
            authError = true;
            console.error(`[poller] ${wf.id} — auth error from operator ${endpoint}: ${err.message}`);
          }
          return null;
        }),
      ]);

      if (taskList?.data) {
        for (const task of taskList.data) {
          await reconcileTaskResource(wf.id, task, db);
        }
        anySuccess = true;
      }

      if (serviceList?.data) {
        for (const service of serviceList.data) {
          await reconcileServiceResource(wf.id, service, db);
        }
        anySuccess = true;
      }

      if (volumeList?.data) {
        for (const volume of volumeList.data) {
          await reconcileVolumeResource(wf.id, volume, db);
        }
        anySuccess = true;
      }
    } catch (err) {
      if (err.status === 404) {
        workflowNotFound = true;
      } else if (err.status === 401) {
        authError = true;
        console.error(`[poller] ${wf.id} — auth error from operator ${endpoint}: ${err.message}`);
      }
    }
  }

  if (authError && !anySuccess) {
    const ts = now();
    const errorMsg = "Authentication failed: operator rejected auth token";
    console.error(`[poller] ${wf.id} — failing workflow: ${errorMsg}`);
    stmts.updateWorkflowDeployError.run(errorMsg, ts, wf.id);
    broadcast(wf.id, "workflow.failed", {
      workflowId: wf.id,
      nodeId: null,
      operatorResourceId: null,
      resourceType: "workflow",
      details: { phase: "Failed", error: errorMsg },
      timestamp: ts,
    });
    await stopAllRunningServices(wf.id, db);
    return;
  }

  if (workflowNotFound && !anySuccess) {
    const ts = now();
    console.error(`[poller] ${wf.id} — workflow not found on operator, failing`);
    stmts.updateWorkflowDeployError.run(
      "Workflow not found on operator",
      ts,
      wf.id
    );
    broadcast(wf.id, "workflow.failed", {
      workflowId: wf.id,
      nodeId: null,
      operatorResourceId: null,
      resourceType: "workflow",
      details: { phase: "Failed", error: "Workflow not found on operator" },
      timestamp: ts,
    });
    await stopAllRunningServices(wf.id, db);
    return;
  }

  if (anySuccess) {
    if (wf.poll_failures > 0) {
      console.log(`[poller] ${wf.id} — poll recovered, resetting failure count`);
      stmts.updateWorkflowPollFailures.run(0, now(), wf.id);
    }
  } else {
    const newFailures = (wf.poll_failures || 0) + 1;
    stmts.incrementWorkflowPollFailures.run(now(), wf.id);
    console.error(`[poller] ${wf.id} — poll failed (${newFailures}/${maxRetries})`);

    if (newFailures >= maxRetries) {
      const ts = now();
      const errorMsg = `Operator unreachable after ${newFailures} attempts`;
      console.error(`[poller] ${wf.id} — failing workflow: ${errorMsg}`);
      stmts.updateWorkflowDeployError.run(errorMsg, ts, wf.id);
      broadcast(wf.id, "workflow.failed", {
        workflowId: wf.id,
        nodeId: null,
        operatorResourceId: null,
        resourceType: "workflow",
        details: { phase: "Failed", error: errorMsg },
        timestamp: ts,
      });
      await stopAllRunningServices(wf.id, db);
    }
  }
}

async function reconcileTaskResource(workflowId, operatorTask, db) {
  const { stmts } = db;
  const operatorPhase = OPERATOR_TASK_PHASE_MAP[operatorTask.status?.phase];
  if (!operatorPhase) return;

  const resourceId = operatorTask.id;
  const nodeRow = stmts.getNodeByOperatorResourceId.get(resourceId);
  if (!nodeRow) return;

  if (nodeRow.status === operatorPhase) return;

  console.log(`[poller] ${workflowId} — task "${nodeRow.section}.${nodeRow.name}" ${nodeRow.status} -> ${operatorPhase}`);

  const event = operatorPhase === "Succeeded" ? "task.succeeded"
    : operatorPhase === "Failed" ? "task.failed"
    : operatorPhase === "Running" ? "task.running"
    : null;

  if (event) {
    await handleWebhook(db, event, workflowId, resourceId, "task", { phase: operatorPhase });
  }
}

async function reconcileServiceResource(workflowId, operatorService, db) {
  const { stmts } = db;
  const operatorPhase = OPERATOR_SERVICE_PHASE_MAP[operatorService.status?.phase];
  if (!operatorPhase) return;

  const resourceId = operatorService.id;
  const nodeRow = stmts.getNodeByOperatorResourceId.get(resourceId);
  if (!nodeRow) return;

  if (nodeRow.status === operatorPhase) return;

  console.log(`[poller] ${workflowId} — service "${nodeRow.section}.${nodeRow.name}" ${nodeRow.status} -> ${operatorPhase}`);

  const event = operatorPhase === "Running" ? "service.running"
    : operatorPhase === "Failed" ? "service.failed"
    : operatorPhase === "Stopped" ? "service.stopped"
    : null;

  if (event) {
    await handleWebhook(db, event, workflowId, resourceId, "service", { phase: operatorPhase });
  }
}

async function reconcileVolumeResource(workflowId, operatorVolume, db) {
  const { stmts } = db;
  const operatorPhase = OPERATOR_VOLUME_PHASE_MAP[operatorVolume.status?.phase];
  if (!operatorPhase) return;

  const resourceId = operatorVolume.id;
  const volRow = stmts.getVolumeByOperatorResourceId.get(resourceId);
  if (!volRow) return;

  if (volRow.status === operatorPhase) return;

  console.log(`[poller] ${workflowId} — volume "${volRow.name}" ${volRow.status} -> ${operatorPhase}`);

  const event = operatorPhase === "Bound" ? "volume.bound"
    : operatorPhase === "Failed" ? "volume.failed"
    : null;

  if (event) {
    await handleWebhook(db, event, workflowId, resourceId, "volume", { phase: operatorPhase });
  }
}

async function checkWorkflowTimeout(wf, db) {
  const timeoutMs = getWorkflowTimeout();
  if (timeoutMs <= 0) return;

  const updatedAt = new Date(wf.updated_at).getTime();
  const elapsed = Date.now() - updatedAt;

  if (elapsed > timeoutMs) {
    const { stmts } = db;
    const ts = now();
    stmts.updateWorkflowDeployError.run(
      `Workflow exceeded timeout of ${timeoutMs}ms`,
      ts,
      wf.id
    );
    broadcast(wf.id, "workflow.failed", {
      workflowId: wf.id,
      nodeId: null,
      operatorResourceId: null,
      resourceType: "workflow",
      details: { phase: "Failed", error: `Workflow exceeded timeout of ${timeoutMs}ms` },
      timestamp: ts,
    });
    await stopAllRunningServices(wf.id, db);
  }
}

export { startPoller, stopPoller, reconcileAll, reconcileWorkflow, checkWorkflowTimeout };
