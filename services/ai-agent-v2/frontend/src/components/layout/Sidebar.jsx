import React from 'react'
import { useStore } from '../../store'
import { AGENTS, AGENT_ORDER } from '../../data/agents'
import clsx from 'clsx'

export default function Sidebar({ onAgentSelect }) {
  const { activeAgent, setActiveAgent, messages, sessionId, newSession } = useStore()

  const handleAgent = (id) => {
    setActiveAgent(id)
    onAgentSelect?.()
  }

  const handleNewSession = async () => {
    const oldId = useStore.getState().sessionId
    try { await fetch(`/api/session?session_id=${oldId}`, { method: 'DELETE' }) } catch {}
    newSession()
    onAgentSelect?.()
  }

  return (
    <div className="flex flex-col h-full py-3 overflow-y-auto">
      {/* New Chat button */}
      <div className="px-3 mb-3">
        <button
          onClick={handleNewSession}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-xl text-xs font-semibold transition-all duration-150 border"
          style={{
            background: 'rgba(91,200,212,0.08)',
            borderColor: 'rgba(91,200,212,0.25)',
            color: '#5BC8D4',
          }}
          onMouseEnter={e => {
            e.currentTarget.style.background = 'rgba(91,200,212,0.15)'
            e.currentTarget.style.borderColor = 'rgba(91,200,212,0.45)'
          }}
          onMouseLeave={e => {
            e.currentTarget.style.background = 'rgba(91,200,212,0.08)'
            e.currentTarget.style.borderColor = 'rgba(91,200,212,0.25)'
          }}
        >
          <span className="text-base leading-none">＋</span>
          <span>New Chat</span>
        </button>
      </div>

      {/* Agent selector */}
      <div className="px-3 mb-1">
        <p className="text-2xs text-ink-muted font-semibold uppercase tracking-widest mb-2 px-1">
          Route to Agent
        </p>
        <nav className="flex flex-col gap-0.5">
          {AGENT_ORDER.map(id => {
            const a = AGENTS[id]
            const isActive = activeAgent === id
            return (
              <button
                key={id}
                onClick={() => handleAgent(id)}
                title={a.description}
                className={clsx(
                  'flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-150 text-left w-full group',
                  isActive
                    ? 'bg-elevated text-ink-primary'
                    : 'text-ink-secondary hover:bg-elevated/60 hover:text-ink-primary'
                )}
                style={isActive ? {
                  borderLeft: `2px solid ${a.accentColor}`,
                  paddingLeft: '10px',
                } : { borderLeft: '2px solid transparent' }}
              >
                <span className="text-base leading-none w-4 text-center shrink-0">
                  {a.icon}
                </span>
                <span className="flex-1 min-w-0 truncate">{a.label}</span>
                {isActive && (
                  <span
                    className="w-1.5 h-1.5 rounded-full shrink-0"
                    style={{ background: a.accentColor }}
                  />
                )}
              </button>
            )
          })}
        </nav>
      </div>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Session info */}
      <div className="px-3 pb-2">
        <div className="divider mb-3" />
        <div className="px-1 space-y-1">
          <p className="text-2xs text-ink-muted font-semibold uppercase tracking-widest mb-1.5">
            Session
          </p>
          <p className="text-2xs text-ink-muted font-mono truncate">
            {sessionId.slice(0, 8)}…
          </p>
          <p className="text-2xs text-ink-muted">
            {messages.length} message{messages.length !== 1 ? 's' : ''}
          </p>
        </div>
      </div>
    </div>
  )
}
