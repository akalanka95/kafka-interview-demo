import { useCallback, useEffect, useState } from 'react'
import { ApiError, resetStats, runAll, sendMessages, type RunAllOutcome } from './api'
import { FeedColumn } from './components/FeedColumn'
import { Header } from './components/Header'
import { ProducerPanel, type Busy, type RunLine } from './components/ProducerPanel'
import { StatsCard } from './components/StatsCard'
import { Toasts } from './components/Toasts'
import { useCluster } from './hooks/useCluster'
import { useEventStream } from './hooks/useEventStream'
import { useStats } from './hooks/useStats'
import { useToasts } from './hooks/useToasts'
import { MODES, MODE_META } from './modes'
import type { MessageRequest, SendResponse } from './types'

function describe(r: SendResponse): RunLine {
  const parts = [`${r.acked.toLocaleString()} acked`, `${r.failed.toLocaleString()} failed`]
  if (r.duplicatesInjected > 0) parts.push(`${r.duplicatesInjected.toLocaleString()} dup injected`)
  if (r.aborted > 0) parts.push(`${r.aborted.toLocaleString()} aborted`)
  parts.push(`${r.elapsedMs.toLocaleString()} ms`)
  return { text: `${MODE_META[r.mode].title}: ${parts.join(' · ')}`, bad: r.failed > 0 }
}

function errorText(e: unknown): { title: string; message?: string } {
  if (e instanceof ApiError) return { title: e.status ? `${e.status} ${e.error}` : e.error, message: e.message }
  return { title: 'Unexpected error', message: e instanceof Error ? e.message : String(e) }
}

export default function App() {
  const { feed, status: streamStatus, clear: clearFeed } = useEventStream()
  const { data: stats, error: statsError, refresh: refreshStats } = useStats()
  const cluster = useCluster()
  const { toasts, push, dismiss } = useToasts()
  const [busy, setBusy] = useState<Busy>(null)
  const [lastRun, setLastRun] = useState<RunLine[]>([])

  const backendDown = statsError !== null
  useEffect(() => {
    if (backendDown) push('error', 'Backend unreachable', 'Retrying every second…')
  }, [backendDown, push])

  const handleSend = useCallback(
    async (req: MessageRequest) => {
      setBusy('send')
      try {
        setLastRun([describe(await sendMessages(req))])
      } catch (e) {
        const { title, message } = errorText(e)
        push('error', title, message)
      } finally {
        setBusy(null)
        refreshStats()
      }
    },
    [push, refreshStats],
  )

  const handleRunAll = useCallback(
    async (req: Omit<MessageRequest, 'mode' | 'simulate'>) => {
      setBusy('runAll')
      try {
        const outcomes: RunAllOutcome[] = await runAll(req)
        setLastRun(
          outcomes.map(({ mode, result, error }) =>
            result ? describe(result) : { text: `${MODE_META[mode].title}: ${error?.error ?? 'failed'}`, bad: true },
          ),
        )
        for (const { mode, error } of outcomes) {
          if (error) push('error', `${MODE_META[mode].title}: ${errorText(error).title}`, error.message)
        }
      } catch (e) {
        const { title, message } = errorText(e)
        push('error', title, message)
      } finally {
        setBusy(null)
        refreshStats()
      }
    },
    [push, refreshStats],
  )

  const handleReset = useCallback(async () => {
    try {
      await resetStats()
      setLastRun([])
      push('info', 'Stats reset')
    } catch (e) {
      const { title, message } = errorText(e)
      push('error', title, message)
    } finally {
      refreshStats()
    }
  }, [push, refreshStats])

  return (
    <div className="app">
      <Header
        brokers={cluster.brokers}
        streamStatus={streamStatus}
        backendDown={backendDown}
        consumersReady={stats?.consumersReady ?? true}
        onResetStats={handleReset}
        onClearFeed={clearFeed}
      />
      <main className="layout">
        <ProducerPanel busy={busy} lastRun={lastRun} onSend={handleSend} onRunAll={handleRunAll} />
        <div className="results">
          <div className="stats-row">
            {MODES.map(mode => (
              <StatsCard key={mode} mode={mode} stats={stats?.modes[mode]} dimmed={stats?.inFlight ?? false} />
            ))}
          </div>
          {stats && stats.droppedForUi > 0 && (
            <p className="dropped-note">
              {stats.droppedForUi.toLocaleString()} events not shown in the feed (UI cap). Counters stay exact.
            </p>
          )}
          <div className="feed-row">
            <FeedColumn title="read_uncommitted" subtitle="sees everything written" events={feed.UNCOMMITTED} />
            <FeedColumn title="read_committed" subtitle="only committed transactions" events={feed.COMMITTED} />
          </div>
        </div>
      </main>
      <Toasts toasts={toasts} onDismiss={dismiss} />
    </div>
  )
}
