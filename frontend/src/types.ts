// Mirrors the backend records in backend/src/main/java/com/example/kafkademo/model.

export type Mode = 'AT_MOST_ONCE' | 'AT_LEAST_ONCE' | 'EXACTLY_ONCE'
export type Simulate = 'NONE' | 'DUPLICATE' | 'ABORT'
export type View = 'UNCOMMITTED' | 'COMMITTED'

export interface MessageRequest {
  text: string
  key: string | null
  mode: Mode
  count: number
  simulate: Simulate
}

export interface RecordResult {
  messageId: string
  partition: number
  /** "unknown" for acks=0. */
  offset: string
}

export interface SendResponse {
  mode: Mode
  requested: number
  acked: number
  failed: number
  duplicatesInjected: number
  aborted: number
  elapsedMs: number
  sample: RecordResult[]
}

export interface MessageEvent {
  view: View
  messageId: string
  text: string | null
  key: string | null
  mode: Mode
  topic: string
  partition: number
  offset: number
  timestamp: number
  duplicate: boolean
  aborted: boolean
  seq: number | null // UI_v2
  worker: number | null // UI_v2
  outOfOrder: boolean // UI_v2
}

export interface ModeStats {
  sent: number
  received: number
  lost: number
  duplicates: number
  aborted: number
  outOfOrder?: number // UI_v2
}

export interface Stats {
  modes: Record<Mode, ModeStats>
  inFlight: boolean
  droppedForUi: number
  consumersReady: boolean
}

export interface Broker {
  id: number
  host: string
  /** null = unknown (cluster call failed). */
  up: boolean | null
}

export interface Cluster {
  brokers: Broker[]
  controllerId: number | null
}

/** Body of every backend error: {"error": "<simple class name>", "message": "..."}. */
export interface ApiErrorBody {
  error: string
  message: string
}
