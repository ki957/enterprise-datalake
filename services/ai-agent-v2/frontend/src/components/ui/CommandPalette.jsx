import React, { useState, useEffect } from 'react'
import { useStore } from '../../store'
import { AGENTS, AGENT_ORDER } from '../../data/agents'
import { SERVICES, resolveUrl } from '../../data/services'
import { QUICK_ACTIONS, PERSONA_ORDER } from '../../data/quickActions'
import { useChat } from '../../hooks/useChat'

export default function CommandPalette() {
  const { isCommandPaletteOpen, closeCommandPalette, setActiveAgent, newSession, addToast } = useStore()
  const { sendMessage } = useChat()
  const [query, setQuery] = useState('')

  useEffect(() => {
    if (isCommandPaletteOpen) setQuery('')
  }, [isCommandPaletteOpen])

  if (!isCommandPaletteOpen) return null

  const q = query.toLowerCase().trim()

  // Build filterable items
  const agentItems = AGENT_ORDER.map(id => ({
    type: 'agent', key: `agent-${id}`,
    label: `Switch to ${AGENTS[id].label} Agent`,
    description: AGENTS[id].description,
    icon: AGENTS[id].icon,
    accentColor: AGENTS[id].accentColor,
    action: () => { setActiveAgent(id); closeCommandPalette() },
  }))

  const serviceItems = SERVICES.map(s => ({
    type: 'service', key: `service-${s.id}`,
    label: `Open ${s.label}`,
    description: s.description,
    icon: s.icon,
    accentColor: '#8899AA',
    action: () => { window.open(resolveUrl(s.url), '_blank'); closeCommandPalette() },
  }))

  const quickItems = PERSONA_ORDER.flatMap(p =>
    QUICK_ACTIONS[p].map((a, i) => ({
      type: 'action', key: `action-${p}-${i}`,
      label: a.label,
      description: p,
      icon: '▷',
      accentColor: '#5BC8D4',
      action: () => { sendMessage(a.question); closeCommandPalette() },
    }))
  )

  const systemItems = [
    {
      type: 'system', key: 'system-new-session',
      label: 'New Session', description: 'Start fresh — clears history and rotates session ID',
      icon: '⊕', accentColor: '#5BC8D4',
      action: async () => {
        const oldId = useStore.getState().sessionId
        try {
          await fetch(`/api/session?session_id=${oldId}`, { method: 'DELETE' })
        } catch {}
        newSession()
        closeCommandPalette()
        addToast({ type: 'info', message: 'New session started' })
      },
    },
  ]

  const all = [...agentItems, ...quickItems, ...serviceItems, ...systemItems]
  const filtered = q
    ? all.filter(i => i.label.toLowerCase().includes(q) || i.description?.toLowerCase().includes(q))
    : all

  const groups = {
    'Agents':  filtered.filter(i => i.type === 'agent'),
    'Actions': filtered.filter(i => i.type === 'action'),
    'Services':filtered.filter(i => i.type === 'service'),
    'System':  filtered.filter(i => i.type === 'system'),
  }

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 animate-fade-in"
        onClick={closeCommandPalette}
      />

      {/* Palette */}
      <div className="fixed top-[15%] left-1/2 -translate-x-1/2 w-full max-w-lg z-50 animate-slide-up px-4">
        <div className="glass rounded-2xl overflow-hidden shadow-glass">
          {/* Search input */}
          <div className="flex items-center gap-3 px-4 py-3.5 border-b border-border/50">
            <span className="text-ink-muted text-sm">⌕</span>
            <input
              autoFocus
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder="Search agents, actions, services…"
              className="flex-1 bg-transparent outline-none text-sm text-ink-primary placeholder-ink-muted"
            />
            <kbd className="text-2xs text-ink-muted border border-border px-1.5 py-0.5 rounded font-mono">
              ESC
            </kbd>
          </div>

          {/* Results */}
          <div className="max-h-[340px] overflow-y-auto">
            {Object.entries(groups).map(([group, items]) => {
              if (!items.length) return null
              return (
                <div key={group}>
                  <p className="px-4 pt-3 pb-1 text-2xs text-ink-muted font-semibold uppercase tracking-widest">
                    {group}
                  </p>
                  {items.map((item) => (
                    <button
                      key={item.key}
                      onClick={item.action}
                      className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-elevated/60 transition-colors text-left"
                    >
                      <span
                        className="text-sm w-5 text-center shrink-0"
                        style={{ color: item.accentColor }}
                      >
                        {item.icon}
                      </span>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm text-ink-primary truncate">{item.label}</p>
                        {item.description && (
                          <p className="text-xs text-ink-muted truncate">{item.description}</p>
                        )}
                      </div>
                    </button>
                  ))}
                </div>
              )
            })}
            {filtered.length === 0 && (
              <p className="text-sm text-ink-muted text-center py-8">No results for "{query}"</p>
            )}
          </div>
        </div>
      </div>
    </>
  )
}
