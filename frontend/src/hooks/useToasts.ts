import { useCallback, useRef, useState } from 'react'

export type ToastKind = 'error' | 'info'

export interface Toast {
  id: number
  kind: ToastKind
  title: string
  message?: string
}

const DURATION_MS: Record<ToastKind, number> = { error: 8000, info: 3000 }

export function useToasts() {
  const [toasts, setToasts] = useState<Toast[]>([])
  const nextId = useRef(1)

  const dismiss = useCallback((id: number) => {
    setToasts(list => list.filter(t => t.id !== id))
  }, [])

  const push = useCallback(
    (kind: ToastKind, title: string, message?: string) => {
      const id = nextId.current++
      setToasts(list => [...list.slice(-3), { id, kind, title, message }])
      setTimeout(() => dismiss(id), DURATION_MS[kind])
    },
    [dismiss],
  )

  return { toasts, push, dismiss }
}
