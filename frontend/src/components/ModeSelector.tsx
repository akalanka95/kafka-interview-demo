import { MODES, MODE_META } from '../modes'
import type { Mode } from '../types'

export function ModeSelector({ value, onChange }: { value: Mode; onChange: (mode: Mode) => void }) {
  return (
    <fieldset className="field mode-selector">
      <legend className="label">Delivery mode</legend>
      {MODES.map(mode => (
        <label key={mode} className={`mode-card${mode === value ? ' selected' : ''}`}>
          <input
            type="radio"
            name="mode"
            value={mode}
            checked={mode === value}
            onChange={() => onChange(mode)}
          />
          {MODE_META[mode].title} <span className="hint">{MODE_META[mode].hint}</span>
        </label>
      ))}
    </fieldset>
  )
}
