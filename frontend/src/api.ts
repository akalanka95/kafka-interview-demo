import { MODES } from './modes'
import type { ApiErrorBody, Cluster, Mode, MessageRequest, SendResponse, Stats } from './types'

export class ApiError extends Error {
  readonly status: number
  readonly error: string

  constructor(status: number, error: string, message: string) {
    super(message)
    this.status = status
    this.error = error
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response
  try {
    res = await fetch(path, init)
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') throw e
    throw new ApiError(0, 'NetworkError', 'backend unreachable')
  }
  if (!res.ok) {
    let body: Partial<ApiErrorBody> = {}
    try {
      body = await res.json()
    } catch {
      // The Vite proxy answers with an empty body when :8080 is down.
    }
    throw new ApiError(
      res.status,
      body.error ?? `HTTP ${res.status}`,
      body.message ?? (body.error ? res.statusText : 'backend unreachable'),
    )
  }
  return (res.status === 204 ? undefined : await res.json()) as T
}

const json = (body: unknown): RequestInit => ({
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
})

export function sendMessages(req: MessageRequest): Promise<SendResponse> {
  return request('/api/messages', json(req))
}

export interface RunAllOutcome {
  mode: Mode
  result?: SendResponse
  error?: ApiError
}

/**
 * POST /api/messages/run-all. Until the backend has that endpoint (404 / 405), falls back to one
 * POST /api/messages per mode, in order, collecting per-mode errors instead of stopping at the first.
 */
export async function runAll(req: Omit<MessageRequest, 'mode' | 'simulate'>): Promise<RunAllOutcome[]> {
  try {
    const results = await request<SendResponse[]>('/api/messages/run-all', json(req))
    return results.map(result => ({ mode: result.mode, result }))
  } catch (e) {
    if (!(e instanceof ApiError) || (e.status !== 404 && e.status !== 405)) throw e
  }
  const outcomes: RunAllOutcome[] = []
  for (const mode of MODES) {
    try {
      outcomes.push({ mode, result: await sendMessages({ ...req, mode, simulate: 'NONE' }) })
    } catch (e) {
      if (!(e instanceof ApiError)) throw e
      outcomes.push({ mode, error: e })
    }
  }
  return outcomes
}

export function getStats(signal?: AbortSignal): Promise<Stats> {
  return request('/api/stats', { signal })
}

export function resetStats(): Promise<void> {
  return request('/api/stats', { method: 'DELETE' })
}

export function getCluster(signal?: AbortSignal): Promise<Cluster> {
  return request('/api/cluster', { signal })
}
