import { useState, type FormEvent } from 'react'
import type { MessageRequest, Mode, Simulate } from '../types'
import { InjectFailure } from './InjectFailure'
import { ModeSelector } from './ModeSelector'

export type Busy = 'send' | 'runAll' | null

export interface RunLine {
  text: string
  bad: boolean
}

interface Props {
  busy: Busy
  lastRun: RunLine[]
  onSend: (req: MessageRequest) => void
  onRunAll: (req: Omit<MessageRequest, 'mode' | 'simulate'>) => void
}

const MAX_COUNT = 10_000

export function ProducerPanel({ busy, lastRun, onSend, onRunAll }: Props) {
  const [text, setText] = useState('Order #1042 created')
  const [key, setKey] = useState('ORD-1042')
  const [countInput, setCountInput] = useState('500')
  const [mode, setMode] = useState<Mode>('AT_MOST_ONCE')
  const [simulate, setSimulate] = useState<Simulate>('NONE')

  const count = Number(countInput)
  const countValid = Number.isInteger(count) && count >= 1 && count <= MAX_COUNT
  const canSend = busy === null && countValid && text.trim() !== ''
  const base = () => ({ text: text.trim(), key: key.trim() || null, count })

  const changeMode = (next: Mode) => {
    setMode(next)
    setSimulate('NONE')
  }

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (canSend) onSend({ ...base(), mode, simulate })
  }

  return (
    <form className="card producer" onSubmit={submit}>
      <h2 className="card-title">Produce</h2>

      <label className="field">
        <span className="label">Message</span>
        <input value={text} maxLength={500} onChange={e => setText(e.target.value)} required />
      </label>

      <div className="field-row">
        <label className="field">
          <span className="label">Key</span>
          <input value={key} maxLength={200} placeholder="none" onChange={e => setKey(e.target.value)} />
        </label>
        <label className="field count">
          <span className="label">Count</span>
          <input
            type="number"
            min={1}
            max={MAX_COUNT}
            value={countInput}
            aria-invalid={!countValid}
            onChange={e => setCountInput(e.target.value)}
          />
        </label>
      </div>

      <ModeSelector value={mode} onChange={changeMode} />
      <InjectFailure mode={mode} value={simulate} onChange={setSimulate} />

      <button type="submit" className="btn btn-primary" disabled={!canSend}>
        {busy === 'send' && <span className="spinner" aria-hidden="true" />}
        {countValid ? `Send ${count.toLocaleString()} ${count === 1 ? 'message' : 'messages'}` : 'Send messages'}
      </button>
      <button type="button" className="btn" disabled={!canSend} onClick={() => onRunAll(base())}>
        {busy === 'runAll' && <span className="spinner" aria-hidden="true" />}
        Run all 3 modes
      </button>

      {!countValid && <p className="form-error">Count must be 1–{MAX_COUNT.toLocaleString()}</p>}
      {lastRun.length > 0 && (
        <ul className="last-run" aria-label="Last run">
          {lastRun.map((line, i) => (
            <li key={i} className={line.bad ? 'bad' : undefined}>
              {line.text}
            </li>
          ))}
        </ul>
      )}
    </form>
  )
}
