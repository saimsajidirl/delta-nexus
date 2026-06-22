import type {
  ApiConfig,
  PipelineRunRequest,
  PipelineRunResponse,
  SessionData
} from "./types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

async function parseApiResponse<T>(response: Response): Promise<T> {
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = payload?.detail;
    const message =
      typeof detail === "string"
        ? detail
        : detail?.message ?? response.statusText ?? "Request failed";
    throw new Error(message);
  }
  return payload as T;
}

export async function fetchSessionData(sessionId: string): Promise<SessionData> {
  const response = await fetch(`${API_BASE_URL}/sessions/${sessionId}/data?limit=100`);
  return parseApiResponse<SessionData>(response);
}

export async function fetchApiConfig(): Promise<ApiConfig> {
  const response = await fetch(`${API_BASE_URL}/config`);
  return parseApiResponse<ApiConfig>(response);
}

export async function runSessionPipeline(
  sessionId: string,
  body: PipelineRunRequest
): Promise<PipelineRunResponse> {
  const response = await fetch(`${API_BASE_URL}/sessions/${sessionId}/pipeline/run`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });
  return parseApiResponse<PipelineRunResponse>(response);
}
