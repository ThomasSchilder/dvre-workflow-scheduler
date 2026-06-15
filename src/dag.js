function resolveDag(workflow) {
  const sections = workflow.sections || {};
  const nodeMap = new Map();
  const sectionNodes = new Map();

  for (const [sectionName, section] of Object.entries(sections)) {
    const sectionNodeIds = [];
    const tasks = section.tasks || {};
    const services = section.services || {};

    for (const [taskName, taskSpec] of Object.entries(tasks)) {
      const id = `${sectionName}.${taskName}`;
      const node = {
        id,
        type: "task",
        section: sectionName,
        name: taskName,
        binding: taskSpec.binding || section.binding || null,
        dependsOn: [...(taskSpec.dependsOn || [])],
      };
      nodeMap.set(id, node);
      sectionNodeIds.push(id);
    }

    for (const [serviceName, serviceSpec] of Object.entries(services)) {
      const id = `${sectionName}.${serviceName}`;
      const node = {
        id,
        type: "service",
        section: sectionName,
        name: serviceName,
        binding: serviceSpec.binding || section.binding || null,
        dependsOn: [...(serviceSpec.dependsOn || [])],
      };
      nodeMap.set(id, node);
      sectionNodeIds.push(id);
    }

    sectionNodes.set(sectionName, sectionNodeIds);

    if (section.executionMode === "sequential") {
      const taskIds = Object.keys(tasks).map((n) => `${sectionName}.${n}`);
      for (let i = 1; i < taskIds.length; i++) {
        const node = nodeMap.get(taskIds[i]);
        node.dependsOn.push(taskIds[i - 1]);
      }
    }
  }

  for (const [sectionName, section] of Object.entries(sections)) {
    const sectionDeps = section.dependsOn || [];
    if (sectionDeps.length === 0) continue;

    const downstreamIds = sectionNodes.get(sectionName) || [];
    const upstreamIds = [];
    for (const depSection of sectionDeps) {
      const ids = sectionNodes.get(depSection);
      if (!ids) {
        throw new Error(
          `Section "${sectionName}" dependsOn "${depSection}", but section "${depSection}" does not exist`
        );
      }
      upstreamIds.push(...ids);
    }

    for (const nodeId of downstreamIds) {
      const node = nodeMap.get(nodeId);
      for (const upstreamId of upstreamIds) {
        if (!node.dependsOn.includes(upstreamId)) {
          node.dependsOn.push(upstreamId);
        }
      }
    }
  }

  for (const [id, node] of nodeMap) {
    const resolved = [];
    for (const dep of node.dependsOn) {
      if (dep.includes(".")) {
        if (!nodeMap.has(dep)) {
          throw new Error(
            `Node "${id}" dependsOn "${dep}", but node "${dep}" does not exist`
          );
        }
        resolved.push(dep);
      } else {
        const qualified = `${node.section}.${dep}`;
        if (!nodeMap.has(qualified)) {
          throw new Error(
            `Node "${id}" dependsOn "${dep}", but node "${qualified}" does not exist in section "${node.section}"`
          );
        }
        resolved.push(qualified);
      }
    }
    node.dependsOn = [...new Set(resolved)];
  }

  const tiers = topologicalSort(nodeMap);

  const nodes = {};
  const tiersOutput = [];
  for (const [tier, nodeIds] of tiers.entries()) {
    tiersOutput.push({ tier, nodes: nodeIds });
    for (const nodeId of nodeIds) {
      const node = nodeMap.get(nodeId);
      nodes[nodeId] = {
        type: node.type,
        section: node.section,
        name: node.name,
        tier,
        dependsOn: node.dependsOn,
        binding: node.binding,
      };
    }
  }

  return { nodes, tiers: tiersOutput };
}

function topologicalSort(nodeMap) {
  const inDegree = new Map();
  const adjacency = new Map();

  for (const [id, node] of nodeMap) {
    inDegree.set(id, node.dependsOn.length);
    adjacency.set(id, []);
  }

  for (const [id, node] of nodeMap) {
    for (const dep of node.dependsOn) {
      if (!adjacency.has(dep)) {
        throw new Error(
          `Node "${id}" dependsOn "${dep}", but node "${dep}" does not exist`
        );
      }
      adjacency.get(dep).push(id);
    }
  }

  const tiers = [];
  let queue = [];
  for (const [id, degree] of inDegree) {
    if (degree === 0) {
      queue.push(id);
    }
  }

  let processed = 0;
  while (queue.length > 0) {
    queue.sort();
    tiers.push(queue);
    processed += queue.length;

    const nextQueue = [];
    for (const id of queue) {
      for (const dependent of adjacency.get(id)) {
        const newDegree = inDegree.get(dependent) - 1;
        inDegree.set(dependent, newDegree);
        if (newDegree === 0) {
          nextQueue.push(dependent);
        }
      }
    }
    queue = nextQueue;
  }

  if (processed < nodeMap.size) {
    const remaining = [];
    for (const [id, degree] of inDegree) {
      if (degree > 0) remaining.push(id);
    }
    throw new Error(
      `Dependency cycle detected involving nodes: ${remaining.join(", ")}`
    );
  }

  return tiers;
}

function computeServiceLifecycle(dag) {
  const nodes = dag.nodes;
  const dependents = new Map();
  for (const [id, node] of Object.entries(nodes)) {
    dependents.set(id, []);
  }
  for (const [id, node] of Object.entries(nodes)) {
    for (const dep of node.dependsOn) {
      dependents.get(dep).push(id);
    }
  }

  const serviceLifecycle = {};

  for (const [id, node] of Object.entries(nodes)) {
    if (node.type !== "service") continue;

    const taskDependents = getTransitiveTaskDependents(id, nodes, dependents);
    const noDependents = taskDependents.length === 0;

    let startBeforeTier = null;
    let stopAfterTier = null;

    if (!noDependents) {
      const dependentTiers = taskDependents.map(
        (depId) => nodes[depId].tier
      );
      startBeforeTier = Math.min(...dependentTiers);
      stopAfterTier = Math.max(...dependentTiers);
    }

    serviceLifecycle[id] = {
      startBeforeTier,
      stopAfterTier,
      noDependents,
    };
  }

  return serviceLifecycle;
}

function getTransitiveTaskDependents(nodeId, nodes, dependents) {
  const visited = new Set();
  const taskDeps = [];
  const stack = [...(dependents.get(nodeId) || [])];

  while (stack.length > 0) {
    const current = stack.pop();
    if (visited.has(current)) continue;
    visited.add(current);

    if (nodes[current].type === "task") {
      taskDeps.push(current);
    }

    for (const dep of dependents.get(current) || []) {
      if (!visited.has(dep)) {
        stack.push(dep);
      }
    }
  }

  return taskDeps;
}

export { resolveDag, computeServiceLifecycle };
