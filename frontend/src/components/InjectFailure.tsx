import { SIMULATE_META, simulateAllowed } from '../modes'
import type { Mode, Simulate } from '../types'

const OPTIONS = Object.keys(SIMULATE_META) as Simulate[]

interface Props {
  mode: Mode
  value: Simulate
  onChange: (simulate: Simulate) => void
}

/** Options that don't apply to the selected mode are disabled; ProducerPanel resets to NONE on mode change. */
export function InjectFailure({ mode, value, onChange }: Props) {
  return (
    <label className="field">
      <span className="label">Inject failure</span>
      <select value={value} onChange={e => onChange(e.target.value as Simulate)}>
        {OPTIONS.map(s => (
          <option key={s} value={s} disabled={!simulateAllowed(s, mode)}>
            {SIMULATE_META[s].label}
          </option>
        ))}
      </select>
    </label>
  )
}
