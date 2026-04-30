import React, { useState } from 'react'
import { useStore } from '../../store'
import { useChat } from '../../hooks/useChat'
import clsx from 'clsx'

const PERSONA_ICONS = {
  'Data Engineer':  '⬡',
  'Data Scientist': '◉',
  'Operations':     '◎',
}
const PERSONA_COLORS = {
  'Data Engineer':  '#5BC8D4',
  'Data Scientist': '#7C6FA0',
  'Operations':     '#10B981',
}

export default function PersonaCard({ persona, actions }) {
  const [expanded, setExpanded] = useState(false)
  const { sendMessage } = useChat()
  const icon  = PERSONA_ICONS[persona]  ?? '◈'
  const color = PERSONA_COLORS[persona] ?? '#8899AA'

  const handleAction = (question) => {
    sendMessage(question)
  }

  return (
    <div
      className="rounded-xl border transition-all duration-200 overflow-hidden"
      style={{ borderColor: expanded ? `${color}30` : '#1E2A45' }}
    >
      {/* Card header */}
      <button
        onClick={() => setExpanded(e => !e)}
        className="w-full flex items-center gap-3 p-4 text-left hover:bg-elevated/50 transition-colors"
      >
        <div
          className="w-8 h-8 rounded-lg flex items-center justify-center shrink-0 text-sm"
          style={{ background: `${color}15`, color }}
        >
          {icon}
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold text-ink-primary">{persona}</p>
          <p className="text-xs text-ink-muted">{actions.length} quick actions</p>
        </div>
        <span className={clsx(
          'text-ink-muted text-xs transition-transform duration-200',
          expanded && 'rotate-180'
        )}>▼</span>
      </button>

      {/* Actions */}
      {expanded && (
        <div className="border-t border-border/40 p-3 grid grid-cols-1 sm:grid-cols-2 gap-1.5">
          {actions.map((action, i) => (
            <button
              key={i}
              onClick={() => handleAction(action.question)}
              className="flex items-start gap-2 p-2.5 rounded-lg text-left bg-base/60 hover:bg-elevated border border-border/40 hover:border-border transition-all duration-150 group"
            >
              <span className="text-xs text-ink-secondary leading-relaxed group-hover:text-ink-primary transition-colors">
                {action.label}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
