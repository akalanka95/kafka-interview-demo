import { getStats } from '../api'
import { usePoll } from './usePoll'

export function useStats(intervalMs = 1000) {
  return usePoll(getStats, intervalMs)
}
