import { useEffect, useRef, useState } from 'react'
import type { StreamStatus } from '../hooks/useEventStream'
import type { Broker } from '../types'
import { BrokerChips } from './BrokerChips'

interface Props {
  brokers: Broker[]
  streamStatus: StreamStatus
  backendDown: boolean
  consumersReady: boolean
  onResetStats: () => void
  onClearFeed: () => void
}

export function Header({ brokers, streamStatus, backendDown, consumersReady, onResetStats, onClearFeed }: Props) {
  return (
    <header className="header">
      <h1 className="title">
        <span className="logo" aria-hidden="true">
          ⁂
        </span>
        Kafka delivery lab
      </h1>
      <div className="header-status">
        {backendDown ? (
          <span className="pill pill-down">backend offline</span>
        ) : (
          !consumersReady && (
            <span className="pill pill-warn" title="Records sent before the listeners own their partitions count as lost">
              consumers starting…
            </span>
          )
        )}
        {streamStatus !== 'live' && <span className="pill pill-unknown">stream {streamStatus}…</span>}
        <BrokerChips brokers={brokers} />
        <Menu onResetStats={onResetStats} onClearFeed={onClearFeed} />
      </div>
    </header>
  )
}

function Menu({ onResetStats, onClearFeed }: Pick<Props, 'onResetStats' | 'onClearFeed'>) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const onPointer = (e: PointerEvent) => {
      if (!ref.current?.contains(e.target as Node)) setOpen(false)
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('pointerdown', onPointer)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('pointerdown', onPointer)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  const choose = (action: () => void) => () => {
    setOpen(false)
    action()
  }

  return (
    <div className="menu" ref={ref}>
      <button
        type="button"
        className="menu-button"
        aria-label="More actions"
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen(o => !o)}
      >
        ⋯
      </button>
      {open && (
        <div className="menu-list" role="menu">
          <button type="button" role="menuitem" onClick={choose(onResetStats)}>
            Reset stats
          </button>
          <button type="button" role="menuitem" onClick={choose(onClearFeed)}>
            Clear feed
          </button>
        </div>
      )}
    </div>
  )
}
