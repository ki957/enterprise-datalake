import React, { useState } from 'react'
import clsx from 'clsx'

export default function CopyButton({ text, size = 'sm' }) {
  const [copied, setCopied] = useState(false)

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard not available
    }
  }

  return (
    <button
      onClick={copy}
      className={clsx(
        'flex items-center gap-1 rounded-md border border-border/60 bg-elevated/80 hover:bg-elevated transition-all duration-150',
        size === 'xs' ? 'px-1.5 py-1 text-2xs' : 'px-2 py-1 text-xs'
      )}
    >
      <span className={copied ? 'text-status-up' : 'text-ink-muted'}>
        {copied ? '✓' : '⎘'}
      </span>
      <span className={copied ? 'text-status-up' : 'text-ink-muted'}>
        {copied ? 'Copied' : 'Copy'}
      </span>
    </button>
  )
}
