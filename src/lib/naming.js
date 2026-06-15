function toOperatorResourceName(workflowId, section, name) {
  return `${workflowId}-${section}-${name}`;
}

function toOperatorVolumeName(workflowId, volumeName) {
  return `${workflowId}-${volumeName}`;
}

function translateDependsOn(dependsOn, section) {
  return dependsOn.map((dep) => {
    if (dep.includes(".")) {
      const [s, n] = dep.split(".");
      return `${s}-${n}`;
    }
    return `${section}-${dep}`;
  });
}

export { toOperatorResourceName, toOperatorVolumeName, translateDependsOn };
