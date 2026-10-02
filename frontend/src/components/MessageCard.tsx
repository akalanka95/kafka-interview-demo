import { memo } from 'react'
import { MODE_META } from '../modes'
import type { MessageEvent } from '../types'

/** A duplicate is a new record at its own offset with the same message-id (docs/frontend.md G14). */
export const MessageCard = memo(function MessageCard({ event }: { event: MessageEvent }) {
  const tooltip = `message-id ${event.messageId}` + (event.key ? ` · key ${event.key}` : '')
  return (
    <li className="message" title={tooltip}>
      <div className="message-line">
        <span className="message-text">{event.text ?? '(empty)'}</span>
        {event.duplicate && <span className="pill pill-down">duplicate</span>}
        {event.aborted && <span className="pill pill-unknown">aborted</span>}
      </div>
      <div className="message-meta">
        p{event.partition} · off {event.offset} ·{' '}
        {event.aborted ? (
          'txn rolled back'
        ) : (
          <span className={event.mode === 'EXACTLY_ONCE' ? 'mode-eos' : undefined}>{MODE_META[event.mode].tag}</span>
        )}
      </div>
    </li>
  )
})
