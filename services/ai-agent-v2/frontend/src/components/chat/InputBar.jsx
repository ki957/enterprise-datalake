import React, { useRef, useEffect, useCallback } from 'react'
import { useStore } from '../../store'
import { AGENTS } from '../../data/agents'
import { useChat } from '../../hooks/useChat'
import clsx from 'clsx'

export default function InputBar() {
  const textareaRef = useRef(null)
  const { activeAgent, isThinking, isQuickStartOpen, toggleQuickStart, messages } = useStore()
  const { sendMessage } = useChat()
  const agent = AGENTS[activeAgent]
  const hasMessages = messages.length > 0

  // Auto-resize textarea
  const resize = () => {
    const el = textareaRef.current
    if (!el) return
    el.style.height = 'auto'
    el.style.height = Math.min(el.scrollHeight, 160) + 'px'
  }

  useEffect(() => {
    textareaRef.current?.focus()
  }, [])

  const submit = useCallback(() => {
    const text = textareaRef.current?.value?.trim()
    if (!text || isThinking) return
    sendMessage(text)
    textareaRef.current.value = ''
    textareaRef.current.style.height = 'auto'
  }, [isThinking, sendMessage])

  const onKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      submit()
    }
  }

  return (
    <div className="px-4 md:px-6 pb-4 pb-safe pt-2 border-t border-border/40 bg-base/80 backdrop-blur-sm">
      {/* Quick Start toggle — only visible once a conversation has started */}
      {hasMessages && (
        <div className="flex justify-center mb-2">
          <button
            onClick={toggleQuickStart}
            className={clsx(
              'flex items-center gap-1.5 px-3 py-1 rounded-full border text-2xs font-medium transition-all duration-150',
              isQuickStartOpen
                ? 'border-cyan-text/40 text-cyan-text bg-cyan-text/8'
                : 'border-border/50 text-ink-muted hover:text-ink-secondary hover:border-border'
            )}
          >
            <span>⚡</span>
            <span>Quick Start</span>
            <span className={clsx('transition-transform duration-200 text-3xs', isQuickStartOpen && 'rotate-180')}>▼</span>
          </button>
        </div>
      )}

      <div
        className="flex items-end gap-2 p-2 rounded-2xl border transition-all duration-150"
        style={{
          background: '#141E35',
          borderColor: '#1E2A45',
        }}
        onFocus={(e) => {
          e.currentTarget.style.borderColor = 'rgba(91,200,212,0.35)'
          e.currentTarget.style.boxShadow = '0 0 0 3px rgba(91,200,212,0.06)'
        }}
        onBlur={(e) => {
          if (!e.currentTarget.contains(e.relatedTarget)) {
            e.currentTarget.style.borderColor = '#1E2A45'
            e.currentTarget.style.boxShadow = 'none'
          }
        }}
      >
        {/* Agent mode indicator */}
        <div className="pb-2 pl-1 shrink-0">
          <div
            className="w-5 h-5 rounded-full flex items-center justify-center text-xs"
            style={{
              background: `${agent.accentColor}18`,
              color:       agent.accentColor,
            }}
            title={`${agent.label} mode`}
          >
            {agent.icon}
          </div>
        </div>

        {/* Textarea */}
        <textarea
          ref={textareaRef}
          rows={1}
          placeholder={agent.placeholder}
          disabled={isThinking}
          onInput={resize}
          onKeyDown={onKeyDown}
          className={clsx(
            'flex-1 bg-transparent border-none outline-none resize-none',
            'text-sm text-ink-primary placeholder-ink-muted',
            'py-2 leading-relaxed min-h-[36px] max-h-[160px]',
            isThinking && 'opacity-50 cursor-not-allowed'
          )}
          style={{ fontFamily: "'Inter', system-ui, sans-serif" }}
        />

        {/* Send button */}
        <button
          onClick={submit}
          disabled={isThinking}
          className={clsx(
            'pb-2 pr-1 shrink-0 transition-all duration-150',
            isThinking ? 'opacity-30 cursor-not-allowed' : 'hover:scale-105 active:scale-95'
          )}
          aria-label="Send"
        >
          <div
            className="w-8 h-8 rounded-full flex items-center justify-center"
            style={{
              background: isThinking
                ? '#1E2A45'
                : 'linear-gradient(135deg, #5BC8D4 0%, #7C6FA0 100%)',
            }}
          >
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              <path d="M1 7h12M7 1l6 6-6 6" stroke="#0A0F1E" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </div>
        </button>
      </div>

      {/* Keyboard hint */}
      <p className="text-2xs text-ink-muted mt-1.5 text-center">
        Enter to send · Shift+Enter for new line · ⌘K commands · ⌘⇧N new session
      </p>
    </div>
  )
}
