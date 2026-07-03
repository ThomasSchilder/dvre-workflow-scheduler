import { OperatorClient } from "../operator-client.js";

function buildOperatorClients(infraMap, authToken) {
  const clients = new Map();
  for (const [, infra] of infraMap) {
    if (!clients.has(infra.endpoint)) {
      clients.set(infra.endpoint, new OperatorClient({ baseUrl: infra.endpoint, authToken }));
    }
  }
  return clients;
}

function getOperatorForBinding(binding, infraMap, operatorClients) {
  const infra = infraMap.get(binding);
  if (!infra) {
    throw new Error(`Infrastructure binding "${binding}" not found in resolved infrastructure`);
  }
  const client = operatorClients.get(infra.endpoint);
  if (!client) {
    throw new Error(`No operator client for endpoint "${infra.endpoint}"`);
  }
  return client;
}

function getVolumeOperators(volName, config, infraMap, operatorClients) {
  const endpoints = new Set();
  for (const [sectionName, section] of Object.entries(config.sections || {})) {
    const sectionVolumes = section.volumes || [];
    const sectionHasVolume = sectionVolumes.includes(volName);

    if (sectionHasVolume) {
      const binding = section.binding;
      if (binding && infraMap.has(binding)) {
        endpoints.add(infraMap.get(binding).endpoint);
      }
    }

    for (const [taskName, taskDef] of Object.entries(section.tasks || {})) {
      const taskVolumes = taskDef.volumes || [];
      const taskMounts = taskDef.volumeMounts || {};
      if (taskVolumes.includes(volName) || volName in taskMounts) {
        const taskBinding = taskDef.binding || section.binding;
        if (taskBinding && infraMap.has(taskBinding)) {
          endpoints.add(infraMap.get(taskBinding).endpoint);
        }
      }
    }

    for (const [serviceName, serviceDef] of Object.entries(section.services || {})) {
      const serviceVolumes = serviceDef.volumes || [];
      const serviceMounts = serviceDef.volumeMounts || {};
      if (serviceVolumes.includes(volName) || volName in serviceMounts) {
        const serviceBinding = serviceDef.binding || section.binding;
        if (serviceBinding && infraMap.has(serviceBinding)) {
          endpoints.add(infraMap.get(serviceBinding).endpoint);
        }
      }
    }
  }
  return [...endpoints].map((ep) => operatorClients.get(ep));
}

function reconstructInfraMap(infraJson) {
  if (!infraJson) return new Map();
  const entries = JSON.parse(infraJson);
  return new Map(entries);
}

function getOperatorClientForNode(node, infraMap, operatorClients) {
  const binding = node.infra_binding;
  if (binding && infraMap.has(binding)) {
    const endpoint = infraMap.get(binding).endpoint;
    const client = operatorClients.get(endpoint);
    if (client) return client;
  }
  return [...operatorClients.values()][0];
}

export { buildOperatorClients, getOperatorForBinding, getVolumeOperators, reconstructInfraMap, getOperatorClientForNode };
