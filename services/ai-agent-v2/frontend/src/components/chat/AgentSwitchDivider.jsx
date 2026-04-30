import React from 'react'
import { AGENTS } from '../../data/agents'

export default function AgentSwitchDivider({ fromAgent, toAgent }) {
  if (!fromAgent || !toAgent || fromAgent === toAgent) return null
  const from = AGENTS[fromAgent]
  const to   = AGENTS[toAgent]
  if (!from || !to) return null

  return (
    <div className="flex items-center gap-3 my-3 px-4 animate-fade-in">
      <div className="flex-1 h-px bg-border/50" />
      <span className="text-2xs text-ink-muted font-medium whitespace-nowrap">
        {from.icon} {from.label} → {to.icon} {to.label}
      </span>
      <div className="flex-1 h-px bg-border/50" />
    </div>
  )
}
