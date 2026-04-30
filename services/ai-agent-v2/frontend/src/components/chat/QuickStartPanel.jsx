import React, { useState } from 'react'
import { useStore } from '../../store'
import { useChat } from '../../hooks/useChat'
import { QUICK_ACTIONS, PERSONA_ORDER } from '../../data/quickActions'
import clsx from 'clsx'

const PERSONA_ICONS  = { 'Data Engineer': '⬡', 'Data Scientist': '◉', 'Operations': '◎' }
const PERSONA_COLORS = { 'Data Engineer': '#5BC8D4', 'Data Scientist': '#7C6FA0', 'Operations': '#10B981' }

function ActionGrid({ actions, color, onSend }) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 gap-1.5 pt-2 pb-1">
      {actions.map((action, i) => (
        <button
          key={i}
          onClick={() => onSend(action.question)}
          className="flex items-start gap-2 p-2.5 rounded-lg text-left bg-base/60 hover:bg-elevated border border-border/40 hover:border-border/80 transition-all duration-150 group"
        >
          <span
            className="mt-0.5 shrink-0 text-xs leading-none"
            style={{ color }}
          >
            ›
          </span>
          <span className="text-xs text-ink-secondary leading-relaxed group-hover:text-ink-primary transition-colors">
            {action.label}
          </span>
        </button>
      ))}
    </div>
  )
}

function PersonaSection({ persona, actions, onSend }) {
  const [open, setOpen] = useState(false)
  const color = PERSONA_COLORS[persona] ?? '#8899AA'
  const icon  = PERSONA_ICONS[persona]  ?? '◈'

  return (
    <div
      className="rounded-xl border overflow-hidden transition-all duration-200"
      style={{ borderColor: open ? `${color}35` : 'rgba(30,42,69,0.7)' }}
    >
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-3 px-3 py-2.5 text-left hover:bg-elevated/40 transition-colors"
      >
        <span className="text-sm" style={{ color }}>{icon}</span>
        <span className="flex-1 text-xs font-semibold text-ink-primary">{persona}</span>
        <span className="text-2xs text-ink-muted mr-1">{actions.length} actions</span>
        <span
          className={clsx('text-ink-muted text-2xs transition-transform duration-200', open && 'rotate-180')}
        >▼</span>
      </button>
      {open && (
        <div className="border-t border-border/30 px-3 pb-3">
          <ActionGrid actions={actions} color={color} onSend={onSend} />
        </div>
      )}
    </div>
  )
}

export default function QuickStartPanel() {
  const { isQuickStartOpen, closeQuickStart } = useStore()
  const { sendMessage } = useChat()

  if (!isQuickStartOpen) return null

  const handleSend = (question) => {
    sendMessage(question)
    closeQuickStart()
  }

  return (
    <div className="border-t border-border/40 bg-base/98 backdrop-blur-md animate-slide-up">
      {/* Header */}
      <div className="flex items-center justify-between px-4 md:px-6 pt-3 pb-2">
        <div className="flex items-center gap-2">
          <span className="text-xs text-cyan-text">⚡</span>
          <span className="text-xs font-semibold text-ink-secondary uppercase tracking-widest">
            Quick Start
          </span>
        </div>
        <button
          onClick={closeQuickStart}
          className="w-5 h-5 flex items-center justify-center rounded text-ink-muted hover:text-ink-primary hover:bg-elevated transition-colors text-xs"
          aria-label="Close quick start"
        >
          ✕
        </button>
      </div>

      {/* Persona sections */}
      <div className="px-4 md:px-6 pb-3 space-y-1.5 max-h-72 overflow-y-auto">
        {PERSONA_ORDER.map(persona => (
          <PersonaSection
            key={persona}
            persona={persona}
            actions={QUICK_ACTIONS[persona]}
            onSend={handleSend}
          />
        ))}
      </div>
    </div>
  )
}
