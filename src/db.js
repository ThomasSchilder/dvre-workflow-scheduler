import { DatabaseSync } from "node:sqlite";
import { mkdirSync, existsSync } from "fs";
import { dirname } from "path";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS workflows (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'Pending',
  config_json TEXT NOT NULL,
  dag_json TEXT,
  infra_json TEXT,
  deploy_error TEXT,
  poll_failures INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
  id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  type TEXT NOT NULL,
  section TEXT NOT NULL,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'Pending',
  tier INTEGER NOT NULL DEFAULT 0,
  depends_on TEXT NOT NULL DEFAULT '[]',
  infra_binding TEXT,
  operator_workflow_id TEXT,
  operator_resource_id TEXT,
  desired_phase TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (workflow_id) REFERENCES workflows(id)
);

CREATE TABLE IF NOT EXISTS volumes (
  id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'Pending',
  operator_resource_id TEXT,
  size TEXT NOT NULL,
  storage_class TEXT,
  access_mode TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (workflow_id) REFERENCES workflows(id)
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workflow_id TEXT NOT NULL,
  node_id TEXT,
  event_type TEXT NOT NULL,
  details TEXT,
  timestamp TEXT NOT NULL,
  FOREIGN KEY (workflow_id) REFERENCES workflows(id)
);

CREATE INDEX IF NOT EXISTS idx_nodes_workflow ON nodes(workflow_id);
CREATE INDEX IF NOT EXISTS idx_nodes_operator_resource ON nodes(operator_resource_id);
CREATE INDEX IF NOT EXISTS idx_volumes_workflow ON volumes(workflow_id);
CREATE INDEX IF NOT EXISTS idx_volumes_operator_resource ON volumes(operator_resource_id);
CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id);
`;

function initDb(dbPath) {
  const dir = dirname(dbPath);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode=WAL");
  db.exec("PRAGMA foreign_keys=ON");
  db.exec(SCHEMA);

  const pollFailuresCol = db.prepare("PRAGMA table_info(workflows)").all();
  if (!pollFailuresCol.some((c) => c.name === "poll_failures")) {
    db.exec("ALTER TABLE workflows ADD COLUMN poll_failures INTEGER NOT NULL DEFAULT 0");
  }

  const authTokenCol = db.prepare("PRAGMA table_info(workflows)").all();
  if (!authTokenCol.some((c) => c.name === "auth_token")) {
    db.exec("ALTER TABLE workflows ADD COLUMN auth_token TEXT");
  }

  const stmts = {
    insertWorkflow: db.prepare(
      "INSERT INTO workflows (id, name, status, config_json, dag_json, infra_json, deploy_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ),
    getWorkflow: db.prepare("SELECT * FROM workflows WHERE id = ?"),
    listWorkflows: db.prepare("SELECT * FROM workflows ORDER BY created_at DESC"),
    updateWorkflowStatus: db.prepare(
      "UPDATE workflows SET status = ?, updated_at = ? WHERE id = ?"
    ),
    updateWorkflowInfra: db.prepare(
      "UPDATE workflows SET infra_json = ?, updated_at = ? WHERE id = ?"
    ),
    updateWorkflowDeployError: db.prepare(
      "UPDATE workflows SET status = 'Failed', deploy_error = ?, updated_at = ? WHERE id = ?"
    ),
    deleteWorkflow: db.prepare("DELETE FROM workflows WHERE id = ?"),
    updateWorkflowDag: db.prepare(
      "UPDATE workflows SET dag_json = ?, status = ?, updated_at = ? WHERE id = ?"
    ),
    getRunningWorkflows: db.prepare(
      "SELECT * FROM workflows WHERE status = 'Running'"
    ),
    updateWorkflowPollFailures: db.prepare(
      "UPDATE workflows SET poll_failures = ?, updated_at = ? WHERE id = ?"
    ),
    incrementWorkflowPollFailures: db.prepare(
      "UPDATE workflows SET poll_failures = poll_failures + 1, updated_at = ? WHERE id = ?"
    ),
    updateWorkflowAuthToken: db.prepare(
      "UPDATE workflows SET auth_token = ?, updated_at = ? WHERE id = ?"
    ),

    insertNode: db.prepare(
      "INSERT INTO nodes (id, workflow_id, type, section, name, status, tier, depends_on, infra_binding, operator_workflow_id, operator_resource_id, desired_phase, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ),
    getNode: db.prepare("SELECT * FROM nodes WHERE id = ?"),
    getNodeByOperatorResourceId: db.prepare(
      "SELECT * FROM nodes WHERE operator_resource_id = ?"
    ),
    listNodesByWorkflow: db.prepare(
      "SELECT * FROM nodes WHERE workflow_id = ? ORDER BY tier, section, name"
    ),
    listNodesByWorkflowAndTier: db.prepare(
      "SELECT * FROM nodes WHERE workflow_id = ? AND tier = ? ORDER BY section, name"
    ),
    updateNodeStatus: db.prepare(
      "UPDATE nodes SET status = ?, updated_at = ? WHERE id = ?"
    ),
    updateNodeOperatorIds: db.prepare(
      "UPDATE nodes SET operator_workflow_id = ?, operator_resource_id = ?, updated_at = ? WHERE id = ?"
    ),
    updateNodeDesiredPhase: db.prepare(
      "UPDATE nodes SET desired_phase = ?, updated_at = ? WHERE id = ?"
    ),
    deleteNodesByWorkflow: db.prepare(
      "DELETE FROM nodes WHERE workflow_id = ?"
    ),

    insertVolume: db.prepare(
      "INSERT INTO volumes (id, workflow_id, name, status, operator_resource_id, size, storage_class, access_mode, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ),
    getVolume: db.prepare("SELECT * FROM volumes WHERE id = ?"),
    getVolumeByOperatorResourceId: db.prepare(
      "SELECT * FROM volumes WHERE operator_resource_id = ?"
    ),
    listVolumesByWorkflow: db.prepare(
      "SELECT * FROM volumes WHERE workflow_id = ? ORDER BY name"
    ),
    updateVolumeStatus: db.prepare(
      "UPDATE volumes SET status = ?, updated_at = ? WHERE id = ?"
    ),
    updateVolumeOperatorId: db.prepare(
      "UPDATE volumes SET operator_resource_id = ?, updated_at = ? WHERE id = ?"
    ),
    deleteVolumesByWorkflow: db.prepare(
      "DELETE FROM volumes WHERE workflow_id = ?"
    ),

    insertEvent: db.prepare(
      "INSERT INTO events (workflow_id, node_id, event_type, details, timestamp) VALUES (?, ?, ?, ?, ?)"
    ),
    listEventsByWorkflow: db.prepare(
      "SELECT * FROM events WHERE workflow_id = ? ORDER BY id ASC"
    ),
    listEventsByWorkflowWithLimit: db.prepare(
      "SELECT * FROM events WHERE workflow_id = ? ORDER BY id DESC LIMIT ?"
    ),
    deleteEventsByWorkflow: db.prepare(
      "DELETE FROM events WHERE workflow_id = ?"
    ),
  };

  return { db, stmts };
}

function generateId(prefix = "wf") {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function now() {
  return new Date().toISOString();
}

function workflowToJSON(row) {
  return {
    id: row.id,
    name: row.name,
    status: row.status,
    config: row.config_json ? JSON.parse(row.config_json) : null,
    dag: row.dag_json ? JSON.parse(row.dag_json) : null,
    infra: row.infra_json ? JSON.parse(row.infra_json) : null,
    deployError: row.deploy_error || null,
    pollFailures: row.poll_failures || 0,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function nodeToJSON(row) {
  return {
    id: row.id,
    workflowId: row.workflow_id,
    type: row.type,
    section: row.section,
    name: row.name,
    status: row.status,
    tier: row.tier,
    dependsOn: JSON.parse(row.depends_on),
    infraBinding: row.infra_binding,
    operatorWorkflowId: row.operator_workflow_id,
    operatorResourceId: row.operator_resource_id,
    desiredPhase: row.desired_phase,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function volumeToJSON(row) {
  return {
    id: row.id,
    workflowId: row.workflow_id,
    name: row.name,
    status: row.status,
    operatorResourceId: row.operator_resource_id,
    size: row.size,
    storageClass: row.storage_class,
    accessMode: row.access_mode,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function eventToJSON(row) {
  return {
    id: row.id,
    workflowId: row.workflow_id,
    nodeId: row.node_id,
    eventType: row.event_type,
    details: row.details ? JSON.parse(row.details) : {},
    timestamp: row.timestamp,
  };
}

export { initDb, generateId, now, workflowToJSON, nodeToJSON, volumeToJSON, eventToJSON };
