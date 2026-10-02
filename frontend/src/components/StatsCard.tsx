import { MODE_META } from '../modes'
import type { Mode, ModeStats } from '../types'

interface Props {
  mode: Mode
  stats: ModeStats | undefined
  dimmed: boolean
}

export function StatsCard({ mode, stats, dimmed }: Props) {
  const tone = !stats || stats.sent === 0 ? 'neutral' : stats.lost + stats.duplicates > 0 ? 'bad' : 'good'
  return (
    <section className={`card stats-card${dimmed ? ' dimmed' : ''}`} aria-busy={dimmed}>
      <h2 className="stats-title">{MODE_META[mode].title}</h2>
      <p className="stats-main">
        <span className="stats-received">{stats ? stats.received.toLocaleString() : '–'}</span>
        <span className="stats-sent"> / {stats ? stats.sent.toLocaleString() : '–'}</span>
      </p>
      <p className={`stats-sub tone-${tone}`}>
        {stats ? `${stats.lost.toLocaleString()} lost · ${stats.duplicates.toLocaleString()} dup` : 'no data'}
      </p>
    </section>
  )
}
