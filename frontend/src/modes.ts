import type { Mode, Simulate } from './types'

export const MODES: Mode[] = ['AT_MOST_ONCE', 'AT_LEAST_ONCE', 'EXACTLY_ONCE']

export const MODE_META: Record<Mode, { title: string; tag: string; hint: string }> = {
  AT_MOST_ONCE: { title: 'At-most-once', tag: 'at-most-once', hint: 'acks=0' },
  AT_LEAST_ONCE: { title: 'At-least-once', tag: 'at-least-once', hint: 'acks=all' },
  EXACTLY_ONCE: { title: 'Exactly-once', tag: 'exactly-once', hint: 'txn' },
}

export const SIMULATE_META: Record<Simulate, { label: string; requires: Mode | null }> = {
  NONE: { label: 'None', requires: null },
  DUPLICATE: { label: 'Duplicate (re-send)', requires: 'AT_LEAST_ONCE' },
  ABORT: { label: 'Abort transaction', requires: 'EXACTLY_ONCE' },
}

export function simulateAllowed(simulate: Simulate, mode: Mode): boolean {
  const requires = SIMULATE_META[simulate].requires
  return requires === null || requires === mode
}
