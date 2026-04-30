import React from 'react'

export default function ErrorBubble({ message }) {
  return (
    <div className="mx-4 md:mx-6 my-2 flex items-start gap-3 p-3.5 rounded-xl border border-red-500/20 bg-red-500/5 animate-slide-up">
      <span className="text-red-400 text-sm shrink-0 mt-0.5">⚠</span>
      <div>
        <p className="text-xs font-semibold text-red-400 mb-0.5">Agent Error</p>
        <p className="text-sm text-ink-secondary">{message}</p>
      </div>
    </div>
  )
}
