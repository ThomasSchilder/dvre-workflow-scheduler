const DEFAULT_TIMEOUT = parseInt(process.env.OPERATOR_TIMEOUT_MS || "10000", 10);
const DEFAULT_RETRY_ATTEMPTS = parseInt(process.env.OPERATOR_RETRY_ATTEMPTS || "3", 10);

export class OperatorClient {
  constructor({ baseUrl, timeout, retryAttempts }) {
    if (!baseUrl) throw new Error("OperatorClient requires baseUrl");
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.timeout = timeout ?? DEFAULT_TIMEOUT;
    this.retryAttempts = retryAttempts ?? DEFAULT_RETRY_ATTEMPTS;
  }

  async healthCheck() {
    return this._get("/api/v1/health");
  }

  async createWorkflow(spec) {
    return this._post("/api/v1/workflows", spec);
  }

  async listWorkflows(phase) {
    const query = phase ? `?phase=${phase}` : "";
    return this._get(`/api/v1/workflows${query}`);
  }

  async getWorkflow(id) {
    return this._get(`/api/v1/workflows/${id}`);
  }

  async deleteWorkflow(id) {
    return this._delete(`/api/v1/workflows/${id}`);
  }

  async createTask(workflowId, spec) {
    return this._post(`/api/v1/workflows/${workflowId}/tasks`, spec);
  }

  async listTasks(workflowId, phase) {
    const query = phase ? `?phase=${phase}` : "";
    return this._get(`/api/v1/workflows/${workflowId}/tasks${query}`);
  }

  async getTask(workflowId, taskId) {
    return this._get(`/api/v1/workflows/${workflowId}/tasks/${taskId}`);
  }

  async deleteTask(workflowId, taskId) {
    return this._delete(`/api/v1/workflows/${workflowId}/tasks/${taskId}`);
  }

  async createService(workflowId, spec) {
    return this._post(`/api/v1/workflows/${workflowId}/services`, spec);
  }

  async listServices(workflowId, phase) {
    const query = phase ? `?phase=${phase}` : "";
    return this._get(`/api/v1/workflows/${workflowId}/services${query}`);
  }

  async getService(workflowId, serviceId) {
    return this._get(`/api/v1/workflows/${workflowId}/services/${serviceId}`);
  }

  async deleteService(workflowId, serviceId) {
    return this._delete(`/api/v1/workflows/${workflowId}/services/${serviceId}`);
  }

  async patchServiceDesiredPhase(workflowId, serviceId, desiredPhase) {
    return this._patch(`/api/v1/workflows/${workflowId}/services/${serviceId}`, { desiredPhase });
  }

  async createVolume(workflowId, spec) {
    return this._post(`/api/v1/workflows/${workflowId}/volumes`, spec);
  }

  async listVolumes(workflowId, phase) {
    const query = phase ? `?phase=${phase}` : "";
    return this._get(`/api/v1/workflows/${workflowId}/volumes${query}`);
  }

  async getVolume(workflowId, volumeId) {
    return this._get(`/api/v1/workflows/${workflowId}/volumes/${volumeId}`);
  }

  async deleteVolume(workflowId, volumeId) {
    return this._delete(`/api/v1/workflows/${workflowId}/volumes/${volumeId}`);
  }

  async registerWebhook(spec) {
    return this._post("/api/v1/webhooks", spec);
  }

  async listWebhooks() {
    return this._get("/api/v1/webhooks");
  }

  async deleteWebhook(webhookId) {
    return this._delete(`/api/v1/webhooks/${webhookId}`);
  }

  async _get(path) {
    return this._request("GET", path);
  }

  async _post(path, body) {
    return this._request("POST", path, body);
  }

  async _patch(path, body) {
    return this._request("PATCH", path, body);
  }

  async _delete(path) {
    return this._request("DELETE", path);
  }

  async _request(method, path, body) {
    const url = `${this.baseUrl}${path}`;
    let lastError;

    for (let attempt = 0; attempt < this.retryAttempts; attempt++) {
      try {
        const opts = {
          method,
          headers: { "Content-Type": "application/json" },
          signal: AbortSignal.timeout(this.timeout),
        };
        if (body !== undefined) {
          opts.body = JSON.stringify(body);
        }

        const response = await fetch(url, opts);

        if (response.status >= 500) {
          lastError = new Error(`Server error: HTTP ${response.status}`);
          if (attempt < this.retryAttempts - 1) {
            await _backoff(attempt);
            continue;
          }
          throw lastError;
        }

        const data = await response.json();

        if (!response.ok) {
          const error = new Error(data.error || `HTTP ${response.status}`);
          error.status = response.status;
          error.details = data.details;
          throw error;
        }

        return data;
      } catch (err) {
        if (err.name === "AbortError" || err.name === "TimeoutError") {
          lastError = new Error(`Request to ${url} timed out after ${this.timeout}ms`);
        } else {
          lastError = err;
        }

        if (err.status && err.status < 500) {
          throw err;
        }

        if (attempt < this.retryAttempts - 1) {
          await _backoff(attempt);
          continue;
        }
      }
    }

    throw lastError;
  }
}

function _backoff(attempt) {
  const delay = 500 * Math.pow(2, attempt);
  return new Promise((resolve) => setTimeout(resolve, delay));
}
