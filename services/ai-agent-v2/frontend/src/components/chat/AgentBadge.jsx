import React from 'react'
import { AGENTS } from '../../data/agents'

export default function AgentBadge({ agentId }) {
  if (!agentId || agentId === 'error') return null
  const agent = AGENTS[agentId]
  if (!agent) return null

  return (
    <div
      className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full mb-2"
      style={{
        background: `${agent.accentColor}12`,
        border:     `1px solid ${agent.accentColor}25`,
      }}
    >
      <span className="text-xs leading-none">{agent.icon}</span>
      <span
        className="text-2xs font-semibold uppercase tracking-widest"
        style={{ color: agent.accentColor }}
      >
        {agent.label} Agent
      </span>
    </div>
  )
}
