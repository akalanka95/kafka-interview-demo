import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * Calls `fetcher` now and every `intervalMs`. A tick is skipped while the previous request is still
 * running; `refresh()` aborts it and fetches immediately (e.g. right after a reset).
 * `fetcher` must be stable (a module-level function), or the interval restarts on every render.
 */
export function usePoll<T>(fetcher: (signal: AbortSignal) => Promise<T>, intervalMs: number) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<Error | null>(null)
  const inFlight = useRef<AbortController | null>(null)

  const run = useCallback(async () => {
    const controller = new AbortController()
    inFlight.current = controller
    try {
      const next = await fetcher(controller.signal)
      if (!controller.signal.aborted) {
        setData(next)
        setError(null)
      }
    } catch (e) {
      if (!controller.signal.aborted) setError(e as Error)
    } finally {
      if (inFlight.current === controller) inFlight.current = null
    }
  }, [fetcher])

  const refresh = useCallback(() => {
    inFlight.current?.abort()
    return run()
  }, [run])

  useEffect(() => {
    // run() only sets state after its fetch resolves, not synchronously.
    // oxlint-disable-next-line react/set-state-in-effect
    run()
    const id = setInterval(() => {
      if (!inFlight.current) run()
    }, intervalMs)
    return () => {
      clearInterval(id)
      inFlight.current?.abort()
      inFlight.current = null
    }
  }, [run, intervalMs])

  return { data, error, refresh }
}
