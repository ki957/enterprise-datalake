import React from 'react'
import { useStore } from '../../store'
import clsx from 'clsx'

const TOAST_STYLES = {
  success: { border: 'border-status-up/30', bg: 'bg-status-up/8',  icon: '✓', color: 'text-status-up'  },
  error:   { border: 'border-red-500/30',   bg: 'bg-red-500/8',    icon: '⚠', color: 'text-red-400'    },
  info:    { border: 'border-cyan-muted/30', bg: 'bg-cyan-dim/50',  icon: 'ℹ', color: 'text-cyan-text'  },
  warning: { border: 'border-amber-500/30', bg: 'bg-amber-500/8',  icon: '!', color: 'text-amber-400'  },
}

function Toast({ toast }) {
  const { removeToast } = useStore()
  const s = TOAST_STYLES[toast.type] ?? TOAST_STYLES.info

  return (
    <div
      className={clsx(
        'flex items-start gap-3 px-4 py-3 rounded-xl border shadow-glass w-72 animate-slide-up',
        s.border, s.bg
      )}
    >
      <span className={clsx('text-sm shrink-0 mt-0.5', s.color)}>{s.icon}</span>
      <p className="flex-1 text-sm text-ink-secondary leading-snug">{toast.message}</p>
      <button
        onClick={() => removeToast(toast.id)}
        className="text-ink-muted hover:text-ink-secondary text-xs shrink-0 mt-0.5"
      >
        ✕
      </button>
    </div>
  )
}

export default function ToastStack() {
  const { toasts } = useStore()
  if (!toasts.length) return null

  return (
    <div className="fixed bottom-6 right-4 md:right-6 z-50 flex flex-col gap-2 items-end">
      {toasts.map(t => <Toast key={t.id} toast={t} />)}
    </div>
  )
}
