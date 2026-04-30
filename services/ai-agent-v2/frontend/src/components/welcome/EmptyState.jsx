import React from 'react'
import PersonaCard from './PersonaCard'
import { QUICK_ACTIONS, PERSONA_ORDER } from '../../data/quickActions'

export default function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-start min-h-full px-4 md:px-8 pt-10 pb-4 animate-fade-in">
      {/* Hero */}
      <div className="text-center mb-8 max-w-lg">
        <div className="flex items-center justify-center mb-5">
          <HeroIcon />
        </div>
        <h1 className="text-2xl md:text-3xl font-semibold text-ink-primary mb-2 tracking-tight">
          <span className="gradient-text">DataLake AI</span>
        </h1>
        <p className="text-sm text-ink-secondary leading-relaxed">
          Agentic assistant for your enterprise data pipeline.
          Ask anything about data quality, pipeline health, schema, or business metrics.
        </p>
        <p className="text-xs text-ink-muted mt-2">
          Powered by LangGraph · Groq Llama-4-Scout · ChromaDB RAG
        </p>
      </div>

      {/* Persona cards */}
      <div className="w-full max-w-2xl space-y-2.5">
        <p className="text-xs text-ink-muted font-semibold uppercase tracking-widest mb-3 text-center">
          Quick start
        </p>
        {PERSONA_ORDER.map(persona => (
          <PersonaCard
            key={persona}
            persona={persona}
            actions={QUICK_ACTIONS[persona]}
          />
        ))}
      </div>

      {/* Keyboard shortcut hint */}
      <p className="mt-6 text-xs text-ink-muted">
        Press <kbd className="px-1.5 py-0.5 rounded bg-elevated border border-border text-2xs font-mono">⌘K</kbd> to open the command palette
      </p>
    </div>
  )
}

function HeroIcon() {
  return (
    <svg width="52" height="52" viewBox="0 0 52 52" fill="none" xmlns="http://www.w3.org/2000/svg">
      <polygon
        points="26,4 48,16 48,36 26,48 4,36 4,16"
        fill="none"
        stroke="url(#heroGrad)"
        strokeWidth="1.5"
        strokeLinejoin="round"
      />
      <circle cx="26" cy="26" r="7" fill="none" stroke="url(#heroGrad)" strokeWidth="1.5" />
      <circle cx="26" cy="26" r="2.5" fill="#5BC8D4" opacity="0.7" />

      {/* Node connections */}
      <line x1="26" y1="4"  x2="26" y2="19" stroke="#5BC8D4" strokeWidth="0.8" opacity="0.3" />
      <line x1="26" y1="33" x2="26" y2="48" stroke="#5BC8D4" strokeWidth="0.8" opacity="0.3" />
      <line x1="4"  y1="16" x2="19" y2="22" stroke="#5BC8D4" strokeWidth="0.8" opacity="0.3" />
      <line x1="33" y1="30" x2="48" y2="36" stroke="#5BC8D4" strokeWidth="0.8" opacity="0.3" />
      <line x1="48" y1="16" x2="33" y2="22" stroke="#5BC8D4" strokeWidth="0.8" opacity="0.3" />
      <line x1="19" y1="30" x2="4"  y2="36" stroke="#5BC8D4" strokeWidth="0.8" opacity="0.3" />

      {/* Corner nodes */}
      <circle cx="26" cy="4"  r="2" fill="#5BC8D4" opacity="0.5" />
      <circle cx="26" cy="48" r="2" fill="#5BC8D4" opacity="0.5" />
      <circle cx="4"  cy="16" r="2" fill="#7C6FA0" opacity="0.5" />
      <circle cx="48" cy="16" r="2" fill="#7C6FA0" opacity="0.5" />
      <circle cx="4"  cy="36" r="2" fill="#7C6FA0" opacity="0.5" />
      <circle cx="48" cy="36" r="2" fill="#7C6FA0" opacity="0.5" />

      <defs>
        <linearGradient id="heroGrad" x1="4" y1="4" x2="48" y2="48" gradientUnits="userSpaceOnUse">
          <stop offset="0%"   stopColor="#5BC8D4" />
          <stop offset="100%" stopColor="#7C6FA0" />
        </linearGradient>
      </defs>
    </svg>
  )
}
