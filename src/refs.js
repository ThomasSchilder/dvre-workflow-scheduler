import { resolveDag } from "./dag.js";

const ASSET_TYPE_MAP = { 0: "dataset", 1: "model", 2: "function", 3: "vm", 4: "cluster" };
const PROTOCOL_MAP = { 0: "http", 1: "ftp", 2: "s3" };

function assetTypeCode(name) {
  for (const [code, label] of Object.entries(ASSET_TYPE_MAP)) {
    if (label === name) return Number(code);
  }
  return -1;
}

export async function resolveInfrastructure(workflow, assetIndexerUrl) {
  const baseUrl = assetIndexerUrl || process.env.ASSET_INDEXER_URL;
  const infra = workflow.infrastructure || {};
  const result = new Map();

  const hasAssetSource = Object.values(infra).some((c) => c.source === "asset");
  if (hasAssetSource && !baseUrl) {
    throw new Error("ASSET_INDEXER_URL is required when infrastructure uses source=asset. Set the ASSET_INDEXER_URL environment variable or pass assetIndexerUrl explicitly.");
  }

  for (const [name, config] of Object.entries(infra)) {
    if (config.source === "direct") {
      result.set(name, {
        type: config.type || "kubernetes",
        endpoint: config.endpoint,
      });
    } else if (config.source === "asset") {
      if (!config.assetId) {
        throw new Error(`Infrastructure "${name}" has source=asset but no assetId`);
      }
      const asset = await fetchAsset(baseUrl, config.assetId);
      const expectedType = assetTypeCode("cluster");
      if (asset.asset_type !== expectedType) {
        throw new Error(
          `Asset ${config.assetId} is not a cluster asset (asset_type=${asset.asset_type}, expected ${expectedType})`
        );
      }
      const metadata = parseMetadata(asset.metadata);
      result.set(name, {
        type: metadata?.type || "kubernetes",
        endpoint: asset.url,
        assetId: config.assetId,
        assetName: asset.name,
      });
    } else {
      throw new Error(`Infrastructure "${name}" has unknown source: "${config.source}"`);
    }
  }

  return result;
}

export function resolveExternalRefs(workflow) {
  return workflow.externalRefs || {};
}

export async function resolveExternalRefSpecs(workflow, assetIndexerUrl) {
  const refDefs = workflow.externalRefs || {};
  const result = new Map();

  const baseUrl = assetIndexerUrl || process.env.ASSET_INDEXER_URL;
  const hasAssetSource = Object.values(refDefs).some((r) => r.source === "asset");
  if (hasAssetSource && !baseUrl) {
    throw new Error("ASSET_INDEXER_URL is required when externalRefs use source=asset. Set the ASSET_INDEXER_URL environment variable or pass assetIndexerUrl explicitly.");
  }

  for (const [name, ref] of Object.entries(refDefs)) {
    if (ref.source === "direct") {
      result.set(name, {
        name,
        protocol: ref.protocol,
        uri: ref.uri,
        ...(ref.credentials ? { credentials: ref.credentials } : {}),
      });
    } else if (ref.source === "asset") {
      if (!ref.assetId) {
        throw new Error(`ExternalRef "${name}" has source=asset but no assetId`);
      }
      const asset = await fetchAsset(baseUrl, ref.assetId);
      result.set(name, {
        name,
        protocol: PROTOCOL_MAP[asset.protocol] || "http",
        uri: asset.url,
        metadata: typeof asset.metadata === "string" ? asset.metadata : JSON.stringify(asset.metadata || {}),
      });
    } else {
      throw new Error(`ExternalRef "${name}" has unknown source: "${ref.source}"`);
    }
  }

  return result;
}

export async function resolveTaskAsset(taskDef, assetIndexerUrl) {
  if (!taskDef.source || taskDef.source === "direct") {
    return taskDef;
  }

  if (taskDef.source === "asset") {
    if (!taskDef.assetId) {
      throw new Error(`Task has source=asset but no assetId`);
    }
    const baseUrl = assetIndexerUrl || process.env.ASSET_INDEXER_URL;
    if (!baseUrl) {
      throw new Error("ASSET_INDEXER_URL is required when tasks use source=asset. Set the ASSET_INDEXER_URL environment variable or pass assetIndexerUrl explicitly.");
    }
    const asset = await fetchAsset(baseUrl, taskDef.assetId);
    const expectedType = assetTypeCode("function");
    if (asset.asset_type !== expectedType) {
      throw new Error(
        `Asset ${taskDef.assetId} is not a function asset (asset_type=${asset.asset_type}, expected ${expectedType})`
      );
    }
    const metadata = parseMetadata(asset.metadata);
    const resolved = { ...taskDef };
    if (!resolved.image && metadata?.image) resolved.image = metadata.image;
    if (!resolved.command && metadata?.command) resolved.command = metadata.command;
    if (!resolved.args && metadata?.args) resolved.args = metadata.args;
    if (metadata?.env) {
      resolved.env = { ...metadata.env, ...(resolved.env || {}) };
    }
    if (!resolved.resources && metadata?.resources) resolved.resources = metadata.resources;
    return resolved;
  }

  throw new Error(`Task has unknown source: "${taskDef.source}"`);
}

export async function resolveServiceAsset(serviceDef, assetIndexerUrl) {
  if (!serviceDef.source || serviceDef.source === "direct") {
    return serviceDef;
  }

  if (serviceDef.source === "asset") {
    if (!serviceDef.assetId) {
      throw new Error(`Service has source=asset but no assetId`);
    }
    const baseUrl = assetIndexerUrl || process.env.ASSET_INDEXER_URL;
    if (!baseUrl) {
      throw new Error("ASSET_INDEXER_URL is required when services use source=asset. Set the ASSET_INDEXER_URL environment variable or pass assetIndexerUrl explicitly.");
    }
    const asset = await fetchAsset(baseUrl, serviceDef.assetId);
    const expectedType = assetTypeCode("function");
    if (asset.asset_type !== expectedType) {
      throw new Error(
        `Asset ${serviceDef.assetId} is not a function asset (asset_type=${asset.asset_type}, expected ${expectedType})`
      );
    }
    const metadata = parseMetadata(asset.metadata);
    const resolved = { ...serviceDef };
    if (!resolved.image && metadata?.image) resolved.image = metadata.image;
    if (!resolved.command && metadata?.command) resolved.command = metadata.command;
    if (!resolved.args && metadata?.args) resolved.args = metadata.args;
    if (metadata?.env) {
      resolved.env = { ...metadata.env, ...(resolved.env || {}) };
    }
    if (!resolved.resources && metadata?.resources) resolved.resources = metadata.resources;
    return resolved;
  }

  throw new Error(`Service has unknown source: "${serviceDef.source}"`);
}

export async function resolveWorkflowNodes(workflow, assetIndexerUrl) {
  const sections = workflow.sections || {};
  const resolved = {};
  for (const [sectionName, section] of Object.entries(sections)) {
    resolved[sectionName] = { ...section };
    if (section.tasks) {
      resolved[sectionName].tasks = {};
      for (const [taskName, taskDef] of Object.entries(section.tasks)) {
        resolved[sectionName].tasks[taskName] = await resolveTaskAsset(taskDef, assetIndexerUrl);
      }
    }
    if (section.services) {
      resolved[sectionName].services = {};
      for (const [serviceName, serviceDef] of Object.entries(section.services)) {
        resolved[sectionName].services[serviceName] = await resolveServiceAsset(serviceDef, assetIndexerUrl);
      }
    }
  }
  return resolved;
}

async function fetchAsset(baseUrl, assetId) {
  const url = `${baseUrl}/api/assets/${assetId}`;
  let response;
  try {
    response = await fetch(url, { signal: AbortSignal.timeout(10000) });
  } catch (err) {
    throw new Error(`Failed to fetch asset ${assetId} from ${baseUrl}: ${err.message}`);
  }
  if (!response.ok) {
    if (response.status === 404) {
      throw new Error(`Asset ${assetId} not found at ${baseUrl}`);
    }
    throw new Error(`Failed to fetch asset ${assetId}: HTTP ${response.status}`);
  }
  return response.json();
}

function parseMetadata(metadataStr) {
  if (!metadataStr) return null;
  try {
    return JSON.parse(metadataStr);
  } catch {
    return null;
  }
}
