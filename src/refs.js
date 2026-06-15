import { resolveDag } from "./dag.js";

const ASSET_TYPE_CLUSTER = 4;

const PROTOCOL_MAP = { 0: "http", 1: "ftp", 2: "s3" };

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
      if (asset.asset_type !== ASSET_TYPE_CLUSTER) {
        throw new Error(
          `Asset ${config.assetId} is not a cluster asset (asset_type=${asset.asset_type}, expected ${ASSET_TYPE_CLUSTER})`
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
