import React from 'react'
import { useStore } from '../../store'
import { AGENTS } from '../../data/agents'

export default function ThinkingIndicator() {
  const { isThinking, thinkingAgent, thinkingMsg } = useStore()
  if (!isThinking) return null

  const agent = thinkingAgent ? AGENTS[thinkingAgent] : null
  const color = agent?.accentColor ?? '#8899AA'

  return (
    <div className="flex items-start gap-3 px-4 md:px-6 py-3 animate-fade-in">
      {/* Avatar placeholder */}
      <div
        className="w-7 h-7 rounded-full shrink-0 flex items-center justify-center text-xs mt-0.5"
        style={{ background: `${color}18`, border: `1px solid ${color}25` }}
      >
        <span style={{ color }}>{agent?.icon ?? '◈'}</span>
      </div>

      {/* Dots + status */}
      <div className="flex flex-col gap-1 pt-1.5">
        <div className="flex items-center gap-1.5">
          {[0, 1, 2].map(i => (
            <span
              key={i}
              className="thinking-dot"
              style={{
                background: color,
                animationDelay: `${i * 0.2}s`,
              }}
            />
          ))}
        </div>
        {thinkingMsg && (
          <p className="text-xs text-ink-muted">{thinkingMsg}</p>
        )}
      </div>
    </div>
  )
}
