import type { MessageEvent } from '../types'
import { MessageCard } from './MessageCard'

interface Props {
  title: string
  subtitle: string
  events: MessageEvent[]
}

export function FeedColumn({ title, subtitle, events }: Props) {
  return (
    <section className="card feed">
      <header className="feed-header">
        <h2 className="card-title">{title}</h2>
        <p className="feed-subtitle">{subtitle}</p>
      </header>
      {events.length === 0 ? (
        <p className="feed-empty">No messages yet</p>
      ) : (
        <ul className="feed-list">
          {events.map(e => (
            <MessageCard key={`${e.view}-${e.messageId}-${e.partition}-${e.offset}`} event={e} />
          ))}
        </ul>
      )}
    </section>
  )
}
