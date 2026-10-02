import { useCallback, useEffect, useReducer, useState } from 'react'
import type { MessageEvent as KafkaEvent, View } from '../types'

export const MAX_CARDS = 200
const RETRY_MS = 2000

export type Feed = Record<View, KafkaEvent[]>
export type StreamStatus = 'connecting' | 'live' | 'reconnecting'

type Action = { type: 'append'; events: KafkaEvent[] } | { type: 'clear' }

const EMPTY: Feed = { UNCOMMITTED: [], COMMITTED: [] }

/** Splits a batch by view, newest first, and keeps the last MAX_CARDS per column. */
function reducer(feed: Feed, action: Action): Feed {
  if (action.type === 'clear') return EMPTY
  const incoming: Feed = { UNCOMMITTED: [], COMMITTED: [] }
  for (let i = action.events.length - 1; i >= 0; i--) {
    const event = action.events[i]
    incoming[event.view].push(event)
  }
  const merge = (view: View) =>
    incoming[view].length === 0 ? feed[view] : [...incoming[view], ...feed[view]].slice(0, MAX_CARDS)
  return { UNCOMMITTED: merge('UNCOMMITTED'), COMMITTED: merge('COMMITTED') }
}

/**
 * One EventSource on /api/stream for the app lifetime. EventSource retries by itself after a dropped
 * connection, but gives up for good when a reconnect gets a non-200 (the Vite proxy answers 5xx while
 * the backend is down), so a closed source is replaced after RETRY_MS.
 */
export function useEventStream() {
  const [feed, dispatch] = useReducer(reducer, EMPTY)
  const [status, setStatus] = useState<StreamStatus>('connecting')

  useEffect(() => {
    let source: EventSource
    let retry: ReturnType<typeof setTimeout> | undefined

    const connect = () => {
      source = new EventSource('/api/stream')
      source.onopen = () => setStatus('live')
      source.addEventListener('batch', e => {
        dispatch({ type: 'append', events: JSON.parse((e as globalThis.MessageEvent<string>).data) })
      })
      source.onerror = () => {
        setStatus('reconnecting')
        if (source.readyState === EventSource.CLOSED) retry = setTimeout(connect, RETRY_MS)
      }
    }

    connect()
    return () => {
      clearTimeout(retry)
      source.close()
    }
  }, [])

  const clear = useCallback(() => dispatch({ type: 'clear' }), [])
  return { feed, status, clear }
}
