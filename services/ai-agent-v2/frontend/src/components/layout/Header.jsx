import React from 'react'
import { useStore } from '../../store'
import { AGENTS } from '../../data/agents'

export default function Header() {
  const { activeAgent, toggleMobileSidebar, openCommandPalette, openCostDashboard, newSession } = useStore()

  const handleNewSession = async () => {
    const oldId = useStore.getState().sessionId
    try { await fetch(`/api/session?session_id=${oldId}`, { method: 'DELETE' }) } catch {}
    newSession()
  }
  const agent = AGENTS[activeAgent]

  return (
    <header className="flex items-center justify-between px-4 md:px-5 h-14 shrink-0 border-b border-border/50 bg-base/90 backdrop-blur-sm z-30">

      {/* Left — branding + mobile menu */}
      <div className="flex items-center gap-3 min-w-0">
        {/* Mobile menu button */}
        <button
          onClick={toggleMobileSidebar}
          className="md:hidden flex flex-col gap-[5px] p-1.5 rounded-md hover:bg-elevated transition-colors"
          aria-label="Open menu"
        >
          <span className="w-5 h-px bg-ink-secondary" />
          <span className="w-3.5 h-px bg-ink-secondary" />
          <span className="w-5 h-px bg-ink-secondary" />
        </button>

        {/* Logo + brand */}
        <div className="flex items-center gap-2.5">
          <BrandIcon />
          <div className="flex items-baseline gap-2 min-w-0">
            <span className="text-base font-semibold text-ink-primary tracking-tight">
              Codincity
            </span>
            <span className="hidden sm:block w-px h-3.5 bg-border" />
            <span className="hidden sm:block text-sm font-normal text-cyan-text">
              DataLake AI
            </span>
          </div>
        </div>
      </div>

      {/* Centre — active agent breadcrumb */}
      <div className="absolute left-1/2 -translate-x-1/2 hidden sm:flex">
        {activeAgent === 'auto' ? (
          <span className="text-xs text-ink-muted font-medium tracking-wide">
            Auto-routing
          </span>
        ) : (
          <div
            className="flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold tracking-wide"
            style={{
              background: `${agent.accentColor}12`,
              border:     `1px solid ${agent.accentColor}30`,
              color:       agent.accentColor,
            }}
          >
            <span>{agent.icon}</span>
            <span>{agent.label} Agent</span>
          </div>
        )}
      </div>

      {/* Right — cost dashboard + command palette + status dots */}
      <div className="flex items-center gap-2">
        {/* New Chat button */}
        <button
          onClick={handleNewSession}
          title="New session (clears chat)"
          className="flex items-center gap-1.5 px-2 py-1 rounded-md bg-elevated border border-border/60 hover:border-border transition-colors"
        >
          <span className="text-xs" style={{ color: '#5BC8D4' }}>＋</span>
          <span className="hidden sm:block text-xs text-ink-muted font-medium">New</span>
        </button>

        {/* Cost tracker button */}
        <button
          onClick={openCostDashboard}
          title="Token cost dashboard"
          className="flex items-center gap-1.5 px-2 py-1 rounded-md bg-elevated border border-border/60 hover:border-border transition-colors"
        >
          <span className="text-xs text-ink-muted">💰</span>
          <span className="hidden sm:block text-xs text-ink-muted font-medium">Cost</span>
        </button>

        {/* Cmd+K hint */}
        <button
          onClick={openCommandPalette}
          className="hidden md:flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-elevated border border-border/60 hover:border-border transition-colors"
        >
          <span className="text-xs text-ink-muted font-mono">⌘K</span>
        </button>

        {/* Health dots (top 3 services) */}
        <HealthDots />
      </div>
    </header>
  )
}

function BrandIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
      <polygon
        points="16,3 29,10 29,22 16,29 3,22 3,10"
        fill="none" stroke="#5BC8D4" strokeWidth="1.5" strokeLinejoin="round"
      />
      <circle cx="16" cy="16" r="3.5" fill="#5BC8D4" opacity="0.85" />
      <line x1="16" y1="3"  x2="16" y2="12.5" stroke="#5BC8D4" strokeWidth="1" opacity="0.4" />
      <line x1="16" y1="19.5" x2="16" y2="29" stroke="#5BC8D4" strokeWidth="1" opacity="0.4" />
      <line x1="3"  y1="10" x2="12.5" y2="14.5" stroke="#5BC8D4" strokeWidth="1" opacity="0.4" />
      <line x1="19.5" y1="17.5" x2="29" y2="22" stroke="#5BC8D4" strokeWidth="1" opacity="0.4" />
      <line x1="29" y1="10" x2="19.5" y2="14.5" stroke="#5BC8D4" strokeWidth="1" opacity="0.4" />
      <line x1="12.5" y1="17.5" x2="3" y2="22" stroke="#5BC8D4" strokeWidth="1" opacity="0.4" />
    </svg>
  )
}

function HealthDots() {
  const { serviceHealth } = useStore()
  const topServices = ['airflow', 'clickhouse', 'grafana']

  return (
    <div className="flex items-center gap-1.5" title="Service health">
      {topServices.map(id => {
        const status = serviceHealth[id] ?? 'unknown'
        return (
          <span
            key={id}
            className={`status-dot ${status}`}
            title={`${id}: ${status}`}
          />
        )
      })}
    </div>
  )
}
