import { resolveDag, computeServiceLifecycle } from "./dag.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { basename, join } from "path";
import { fileURLToPath } from "url";
import { instance } from "@viz-js/viz";

function sanitizeId(id) {
  return id.replace(/[^a-zA-Z0-9]/g, "_");
}

export { sanitizeId, toDot };

function buildSectionBindingInfo(sections, dag) {
  const info = {};
  for (const [sectionName, section] of Object.entries(sections || {})) {
    const sectionBinding = section.binding || null;
    const nodeBindings = new Map();
    for (const [id, node] of Object.entries(dag.nodes)) {
      if (node.section !== sectionName) continue;
      const spec = getNodeSpec(sections, sectionName, node.type, node.name);
      nodeBindings.set(id, spec?.binding || sectionBinding);
    }
    const bindings = [...new Set(nodeBindings.values())];
    const uniform = bindings.length === 1 ? bindings[0] : null;
    info[sectionName] = { sectionBinding, uniform, nodeBindings };
  }
  return info;
}

function getNodeSpec(sections, sectionName, type, name) {
  const section = sections?.[sectionName];
  if (!section) return null;
  const group = type === "service" ? section.services : section.tasks;
  return group?.[name] || null;
}

function buildVolumeConsumers(workflow, sections, dag) {
  const volumes = workflow.volumes || {};
  const result = {};
  for (const volName of Object.keys(volumes)) {
    const sectionsConsumers = {};
    for (const [id, node] of Object.entries(dag.nodes)) {
      const spec = getNodeSpec(sections, node.section, node.type, node.name);
      const volumeMounts = spec?.volumeMounts || {};
      if (!(volName in volumeMounts)) continue;
      if (!sectionsConsumers[node.section]) {
        sectionsConsumers[node.section] = { nodes: new Map(), total: 0 };
      }
      sectionsConsumers[node.section].nodes.set(id, volumeMounts[volName]);
    }
    for (const [sectionName, section] of Object.entries(sections || {})) {
      const tasks = Object.keys(section.tasks || {});
      const services = Object.keys(section.services || {});
      const total = tasks.length + services.length;
      const consumerCount = sectionsConsumers[sectionName]?.nodes.size || 0;
      if (sectionsConsumers[sectionName]) {
        sectionsConsumers[sectionName].total = total;
        sectionsConsumers[sectionName].ubiquitous = consumerCount === total;
      }
    }
    result[volName] = { spec: volumes[volName], sections: sectionsConsumers };
  }
  return result;
}

function buildExtRefConsumers(workflow, sections, dag) {
  const refs = workflow.externalRefs || {};
  const result = {};
  for (const refName of Object.keys(refs)) {
    const sectionsConsumers = {};
    for (const [id, node] of Object.entries(dag.nodes)) {
      const spec = getNodeSpec(sections, node.section, node.type, node.name);
      const extRefs = spec?.externalRefs || [];
      if (!extRefs.includes(refName)) continue;
      if (!sectionsConsumers[node.section]) {
        sectionsConsumers[node.section] = { nodes: new Set(), total: 0 };
      }
      sectionsConsumers[node.section].nodes.add(id);
    }
    for (const [sectionName, section] of Object.entries(sections || {})) {
      const tasks = Object.keys(section.tasks || {});
      const services = Object.keys(section.services || {});
      const total = tasks.length + services.length;
      const consumerCount = sectionsConsumers[sectionName]?.nodes.size || 0;
      if (sectionsConsumers[sectionName]) {
        sectionsConsumers[sectionName].total = total;
        sectionsConsumers[sectionName].ubiquitous = consumerCount === total;
      }
    }
    result[refName] = { spec: refs[refName], sections: sectionsConsumers };
  }
  return result;
}

function formatAccessMode(mode) {
  const map = { ReadWriteOnce: "RWO", ReadWriteMany: "RWM", ReadOnlyMany: "ROM" };
  return map[mode] || mode || "RWO";
}

function formatExtRefSource(spec) {
  if (spec.source === "asset") return `asset #${spec.assetId}`;
  if (spec.protocol) return spec.protocol;
  return spec.source;
}

function toDot(dag, lifecycle, metadata, workflow) {
  const lines = [];
  const name = metadata?.name || "workflow";
  const sections = workflow.sections || {};

  lines.push(`digraph "${name}" {`);
  lines.push(`  rankdir=TB;`);
  lines.push(`  newrank=true;`);
  lines.push(`  compound=true;`);
  lines.push(`  nodesep=0.6;`);
  lines.push(`  ranksep=0.75;`);
  lines.push(`  fontname="Helvetica";`);
  lines.push(`  node [fontname="Helvetica", fontsize=11];`);
  lines.push(`  edge [fontname="Helvetica"];`);
  lines.push(``);

  const sectionDeps = {};
  for (const [sectionName, section] of Object.entries(sections)) {
    if (section.dependsOn && section.dependsOn.length > 0) {
      sectionDeps[sectionName] = new Set(section.dependsOn);
    }
  }

  const bindingInfo = buildSectionBindingInfo(sections, dag);
  const volumeConsumers = buildVolumeConsumers(workflow, sections, dag);
  const extRefConsumers = buildExtRefConsumers(workflow, sections, dag);

  const nodeUbiquitousVolumes = {};
  const nodeUbiquitousExtRefs = {};
  for (const [volName, vc] of Object.entries(volumeConsumers)) {
    for (const [sectionName, sc] of Object.entries(vc.sections)) {
      if (sc.ubiquitous) {
        for (const [nodeId, mountPath] of sc.nodes) {
          if (!nodeUbiquitousVolumes[nodeId]) nodeUbiquitousVolumes[nodeId] = [];
          nodeUbiquitousVolumes[nodeId].push(`${volName}:${mountPath}`);
        }
      }
    }
  }
  for (const [refName, rc] of Object.entries(extRefConsumers)) {
    for (const [sectionName, sc] of Object.entries(rc.sections)) {
      if (sc.ubiquitous) {
        for (const nodeId of sc.nodes) {
          if (!nodeUbiquitousExtRefs[nodeId]) nodeUbiquitousExtRefs[nodeId] = [];
          nodeUbiquitousExtRefs[nodeId].push(`externalRef:${refName}`);
        }
      }
    }
  }

  const maxTier = Math.max(...Object.values(dag.nodes).map((n) => n.tier));
  for (let t = 0; t <= maxTier; t++) {
    lines.push(`  _tier_${t} [label="Tier ${t}", shape=plaintext, fontsize=11, fontcolor="#999"];`);
  }
  if (maxTier > 0) {
    const chain = [];
    for (let t = 0; t <= maxTier; t++) chain.push(`_tier_${t}`);
    lines.push(`  ${chain.join(" -> ")} [style=invis];`);
  }
  lines.push(``);

  const sectionGroups = {};
  for (const [id, node] of Object.entries(dag.nodes)) {
    if (!sectionGroups[node.section]) sectionGroups[node.section] = [];
    sectionGroups[node.section].push({ id, ...node });
  }

  const sectionRepNodes = {};
  for (const [section, nodes] of Object.entries(sectionGroups)) {
    const sorted = [...nodes].sort((a, b) => a.tier - b.tier || a.name.localeCompare(b.name));
    sectionRepNodes[section] = sanitizeId(sorted[0].id);
  }

  for (const [section, nodes] of Object.entries(sectionGroups)) {
    const bi = bindingInfo[section] || {};
    const sectionLabel = bi.uniform
      ? `${section.charAt(0).toUpperCase() + section.slice(1)} [${bi.uniform}]`
      : section.charAt(0).toUpperCase() + section.slice(1);

    lines.push(`  subgraph cluster_${sanitizeId(section)} {`);
    lines.push(`    label="${sectionLabel}";`);
    lines.push(`    labeljust=l;`);
    lines.push(`    style=filled;`);
    lines.push(`    color="#DDEEFF";`);
    lines.push(`    fillcolor="#F5F8FC";`);
    lines.push(`    fontcolor="#333";`);
    lines.push(`    fontsize=14;`);
    lines.push(`    penwidth=1.5;`);
    lines.push(``);

    for (const node of nodes) {
      const sid = sanitizeId(node.id);
      const lc = lifecycle[node.id];
      const noDependents = lc?.noDependents === true;
      const labelParts = [node.name];
      const nodeBinding = bi.nodeBindings?.get(node.id);
      if (!bi.uniform && nodeBinding) {
        labelParts.push(`[${nodeBinding}]`);
      }
      const ubiqVols = nodeUbiquitousVolumes[node.id] || [];
      for (const v of ubiqVols) labelParts.push(v);
      const ubiqRefs = nodeUbiquitousExtRefs[node.id] || [];
      for (const r of ubiqRefs) labelParts.push(r);

      if (node.type === "service") {
        if (noDependents) {
          lines.push(`    ${sid} [label="${labelParts.join("\\n")}", shape=ellipse, style="filled,dashed", fillcolor="#F0AD4E", color="#D4880E", fontcolor="#fff", penwidth=2];`);
        } else {
          lines.push(`    ${sid} [label="${labelParts.join("\\n")}", shape=ellipse, style=filled, fillcolor="#5CB85C", fontcolor="#fff"];`);
        }
      } else {
        lines.push(`    ${sid} [label="${labelParts.join("\\n")}", shape=box, style="filled,rounded", fillcolor="#4A90D9", fontcolor="#fff", color="#2C5F8A"];`);
      }
    }
    lines.push(`  }`);
    lines.push(``);
  }

  const tierGroups = {};
  for (const [id, node] of Object.entries(dag.nodes)) {
    if (!tierGroups[node.tier]) tierGroups[node.tier] = [];
    tierGroups[node.tier].push(sanitizeId(id));
  }
  for (const [tier, ids] of Object.entries(tierGroups)) {
    for (const sid of ids) {
      lines.push(`  _tier_${tier} -> ${sid} [style=invis];`);
    }
  }
  lines.push(``);

  const drawnSectionEdges = new Set();
  for (const [id, node] of Object.entries(dag.nodes)) {
    for (const dep of node.dependsOn) {
      const depNode = dag.nodes[dep];
      if (!depNode) continue;

      const depSection = depNode.section;
      const nodeSection = node.section;

      if (depSection !== nodeSection && sectionDeps[nodeSection]?.has(depSection)) {
        const key = `${depSection}->${nodeSection}`;
        if (!drawnSectionEdges.has(key)) {
          drawnSectionEdges.add(key);
          lines.push(`  ${sectionRepNodes[depSection]} -> ${sectionRepNodes[nodeSection]} [ltail="cluster_${sanitizeId(depSection)}", lhead="cluster_${sanitizeId(nodeSection)}", style=bold, penwidth=2, color="#666666"];`);
        }
      } else {
        lines.push(`  ${sanitizeId(dep)} -> ${sanitizeId(id)};`);
      }
    }
  }

  lines.push(`}`);
  return lines.join("\n");
}

async function renderDotToSvg(dotSource, svgPath) {
  try {
    const viz = await instance();
    const svg = viz.renderString(dotSource, { format: "svg" });
    writeFileSync(svgPath, svg);
    console.log(`Wrote ${svgPath}`);
  } catch (err) {
    console.error(`Error: failed to render SVG: ${err.message}`);
    process.exit(1);
  }
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0 || args.includes("--help") || args.includes("-h")) {
    console.log(`Usage: node src/viz.js <workflow.json> [options]

Options:
  --output <dir>   Write .dot and .svg files to directory instead of stdout
  --svg            Also render DOT to SVG (requires --output)

Examples:
  node src/viz.js schemas/examples/climate-pipeline.json
  node src/viz.js climate.json --output ./figures
  node src/viz.js climate.json --output ./figures --svg`);
    process.exit(0);
  }

  let outputDir = null;
  let renderSvg = false;
  let filePath = null;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--output" && args[i + 1]) {
      outputDir = args[++i];
    } else if (args[i] === "--svg") {
      renderSvg = true;
    } else if (!args[i].startsWith("--")) {
      filePath = args[i];
    }
  }

  if (!filePath) {
    console.error(`Error: no workflow JSON file specified`);
    process.exit(1);
  }

  if (renderSvg && !outputDir) {
    console.error(`Error: --svg requires --output directory`);
    process.exit(1);
  }

  let workflow;
  try {
    workflow = JSON.parse(readFileSync(filePath, "utf-8"));
  } catch (err) {
    console.error(`Error: cannot read ${filePath}: ${err.message}`);
    process.exit(1);
  }

  let dag;
  try {
    dag = resolveDag(workflow);
  } catch (err) {
    console.error(`Error: DAG resolution failed: ${err.message}`);
    process.exit(1);
  }

  const lifecycle = computeServiceLifecycle(dag);
  const metadata = workflow.metadata || {};
  const baseName = basename(filePath, ".json");

  const dotOutput = toDot(dag, lifecycle, metadata, workflow);

  if (outputDir) {
    if (!existsSync(outputDir)) {
      mkdirSync(outputDir, { recursive: true });
    }

    const dotPath = join(outputDir, `${baseName}.dot`);
    writeFileSync(dotPath, dotOutput + "\n");
    console.log(`Wrote ${dotPath}`);

    if (renderSvg) {
      await renderDotToSvg(dotOutput, join(outputDir, `${baseName}.svg`));
    }
  } else {
    console.log(dotOutput);
  }
}

const isMainModule = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];

if (isMainModule) {
  main().catch((err) => {
    console.error(`Fatal error: ${err.message}`);
    process.exit(1);
  });
}
