import type { Broker } from '../types'

export function BrokerChips({ brokers }: { brokers: Broker[] }) {
  return (
    <ul className="chips" aria-label="Brokers">
      {brokers.map(b => {
        const state = b.up === null ? 'unknown' : b.up ? 'up' : 'down'
        return (
          <li key={b.id} className={`pill pill-${state}`} title={b.host}>
            kafka-{b.id} {state}
          </li>
        )
      })}
    </ul>
  )
}
