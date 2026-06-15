import { toOperatorResourceName, toOperatorVolumeName, translateDependsOn } from "./naming.js";

function buildTaskSpec(workflowId, nodeId, node, config, externalRefSpecs) {
  const [section, name] = [node.section, node.name];
  const sectionDef = config.sections[section] || {};
  const taskDef = sectionDef.tasks?.[name] || {};
  const taskName = toOperatorResourceName(workflowId, section, name);

  const volumes = [...new Set([
    ...(sectionDef.volumes || []),
    ...(taskDef.volumes || []),
  ])].map((v) => toOperatorVolumeName(workflowId, v));

  const volumeMounts = {};
  const sectionMounts = sectionDef.volumeMounts || {};
  const taskMounts = taskDef.volumeMounts || {};
  for (const [vol, path] of Object.entries(sectionMounts)) {
    volumeMounts[toOperatorVolumeName(workflowId, vol)] = path;
  }
  for (const [vol, path] of Object.entries(taskMounts)) {
    volumeMounts[toOperatorVolumeName(workflowId, vol)] = path;
  }

  const externalRefs = buildExternalRefSpecs(taskDef.externalRefs, externalRefSpecs);

  return {
    taskName,
    image: taskDef.image,
    ...(taskDef.command ? { command: taskDef.command } : {}),
    ...(taskDef.args ? { args: taskDef.args } : {}),
    ...(taskDef.env ? { env: taskDef.env } : {}),
    ...(externalRefs.length > 0 ? { externalRefs } : {}),
    ...(volumes.length > 0 ? { volumes } : {}),
    ...(Object.keys(volumeMounts).length > 0 ? { volumeMounts } : {}),
    ...(taskDef.resources ? { resources: taskDef.resources } : {}),
    ...(node.dependsOn.length > 0 ? { dependsOn: translateDependsOn(node.dependsOn, section) } : {}),
    ...(node.binding ? { infraRef: node.binding } : {}),
  };
}

function buildServiceSpec(workflowId, nodeId, node, config, externalRefSpecs, desiredPhase) {
  const [section, name] = [node.section, node.name];
  const sectionDef = config.sections[section] || {};
  const serviceDef = sectionDef.services?.[name] || {};
  const serviceName = toOperatorResourceName(workflowId, section, name);

  const volumes = [...new Set([
    ...(sectionDef.volumes || []),
    ...(serviceDef.volumes || []),
  ])].map((v) => toOperatorVolumeName(workflowId, v));

  const volumeMounts = {};
  const sectionMounts = sectionDef.volumeMounts || {};
  const serviceMounts = serviceDef.volumeMounts || {};
  for (const [vol, path] of Object.entries(sectionMounts)) {
    volumeMounts[toOperatorVolumeName(workflowId, vol)] = path;
  }
  for (const [vol, path] of Object.entries(serviceMounts)) {
    volumeMounts[toOperatorVolumeName(workflowId, vol)] = path;
  }

  const externalRefs = buildExternalRefSpecs(serviceDef.externalRefs, externalRefSpecs);

  return {
    serviceName,
    image: serviceDef.image,
    ...(serviceDef.command ? { command: serviceDef.command } : {}),
    ...(serviceDef.args ? { args: serviceDef.args } : {}),
    ...(serviceDef.env ? { env: serviceDef.env } : {}),
    ...(externalRefs.length > 0 ? { externalRefs } : {}),
    ...(volumes.length > 0 ? { volumes } : {}),
    ...(Object.keys(volumeMounts).length > 0 ? { volumeMounts } : {}),
    ...(serviceDef.resources ? { resources: serviceDef.resources } : {}),
    ...(node.dependsOn.length > 0 ? { dependsOn: translateDependsOn(node.dependsOn, section) } : {}),
    ...(serviceDef.port ? { port: serviceDef.port } : {}),
    replicas: serviceDef.replicas || 1,
    desiredPhase: desiredPhase || "Running",
    ...(node.binding ? { infraRef: node.binding } : {}),
  };
}

function buildVolumeSpec(workflowId, volName, volDef) {
  return {
    volumeName: toOperatorVolumeName(workflowId, volName),
    size: volDef.size,
    ...(volDef.storageClass ? { storageClass: volDef.storageClass } : {}),
    ...(volDef.accessMode ? { accessMode: volDef.accessMode } : {}),
  };
}

function buildExternalRefSpecs(refNames, externalRefSpecs) {
  if (!refNames || refNames.length === 0) return [];
  const result = [];
  for (const name of refNames) {
    const spec = externalRefSpecs.get(name);
    if (spec) {
      result.push({ ...spec });
    }
  }
  return result;
}

export { buildTaskSpec, buildServiceSpec, buildVolumeSpec, buildExternalRefSpecs };
