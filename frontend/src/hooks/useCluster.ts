import { getCluster } from '../api'
import type { Cluster } from '../types'
import { usePoll } from './usePoll'

/** Shown before the first answer, and whenever /api/cluster fails or doesn't exist yet. */
const UNKNOWN: Cluster = {
  brokers: [1, 2, 3].map(id => ({ id, host: `localhost:${id}9092`, up: null })),
  controllerId: null,
}

async function fetchCluster(signal: AbortSignal): Promise<Cluster> {
  try {
    return await getCluster(signal)
  } catch (e) {
    if (signal.aborted) throw e
    return UNKNOWN
  }
}

export function useCluster(intervalMs = 2000): Cluster {
  return usePoll(fetchCluster, intervalMs).data ?? UNKNOWN
}
