function isTierComplete(tierNum, allNodes, tiers) {
  const tierNodeIds = tiers.find((t) => t.tier === tierNum)?.nodes || [];
  const tasksInTier = allNodes.filter(
    (n) => n.type === "task" && tierNodeIds.includes(`${n.section}.${n.name}`)
  );
  if (tasksInTier.length === 0) return true;
  return tasksInTier.every((n) => n.status === "Succeeded");
}

function getHighestCompletedTier(allNodes, tiers) {
  let highest = -1;
  for (const t of tiers) {
    if (isTierComplete(t.tier, allNodes, tiers)) {
      highest = t.tier;
    } else {
      break;
    }
  }
  return highest;
}

function hasTaskFailed(allNodes) {
  return allNodes.some((n) => n.type === "task" && n.status === "Failed");
}

function isWorkflowComplete(allNodes, tiers) {
  for (const t of tiers) {
    const tierNodeIds = t.nodes || [];
    const tasksInTier = allNodes.filter(
      (n) => n.type === "task" && tierNodeIds.includes(`${n.section}.${n.name}`)
    );
    if (tasksInTier.length > 0 && !tasksInTier.every((n) => n.status === "Succeeded")) {
      return false;
    }
  }
  return tiers.length > 0;
}

function getServicesToStart(nextTier, allNodes, serviceLifecycle) {
  const result = [];
  for (const node of allNodes) {
    if (node.type !== "service") continue;
    const nodeId = `${node.section}.${node.name}`;
    const lc = serviceLifecycle[nodeId];
    if (!lc || lc.noDependents) continue;
    if (lc.startBeforeTier === nextTier && node.desired_phase !== "Running") {
      result.push(node);
    }
  }
  return result;
}

function getServicesToStop(nextTier, allNodes, serviceLifecycle) {
  const result = [];
  for (const node of allNodes) {
    if (node.type !== "service") continue;
    const nodeId = `${node.section}.${node.name}`;
    const lc = serviceLifecycle[nodeId];
    if (!lc || lc.noDependents) continue;
    if (lc.stopAfterTier !== null && lc.stopAfterTier < nextTier && node.desired_phase !== "Stopped") {
      result.push(node);
    }
  }
  return result;
}

export {
  isTierComplete,
  getHighestCompletedTier,
  hasTaskFailed,
  isWorkflowComplete,
  getServicesToStart,
  getServicesToStop,
};
