import React, { useState, Suspense, lazy } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import AgentBadge from './AgentBadge'
import CopyButton from '../ui/CopyButton'
import { PLOTLY_LAYOUT, PLOTLY_CONFIG } from '../../styles/plotlyTheme'

const Plot = lazy(() => import('react-plotly.js'))

// Split content on ```plotly fences — same logic as original Streamlit app
function parseContent(text) {
  const parts = text.split(/```plotly\n([\s\S]*?)\n```/)
  return parts.map((part, i) => ({
    type: i % 2 === 0 ? 'markdown' : 'plotly',
    content: part,
  })).filter(p => p.content.trim())
}

function PlotlyChart({ json }) {
  try {
    const fig = JSON.parse(json)
    return (
      <Suspense fallback={<div className="h-48 flex items-center justify-center text-ink-muted text-sm">Loading chart…</div>}>
        <div className="rounded-lg overflow-hidden border border-border/50 my-3">
          <Plot
            data={fig.data ?? []}
            layout={{ ...PLOTLY_LAYOUT, ...(fig.layout ?? {}), autosize: true }}
            config={PLOTLY_CONFIG}
            style={{ width: '100%', minHeight: '240px' }}
            useResizeHandler
          />
        </div>
      </Suspense>
    )
  } catch {
    return (
      <pre className="code-block text-xs my-2 overflow-x-auto">{json}</pre>
    )
  }
}

function MarkdownContent({ text }) {
  return (
    <div className="prose-message">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          code({ node, className, children, ...props }) {
            const value = String(children).replace(/\n$/, '')
            const lang = (className ?? '').replace('language-', '')
            // react-markdown v9: no inline prop — detect inline by absence of language + no newlines
            const isInline = !lang && !value.includes('\n')
            if (isInline) {
              return <code {...props}>{value}</code>
            }
            return (
              <div className="relative my-2 group">
                <div className="code-block">
                  {lang && (
                    <div className="text-2xs text-cyan-text/60 font-semibold uppercase tracking-widest mb-2">
                      {lang}
                    </div>
                  )}
                  <pre className="overflow-x-auto m-0">
                    <code>{value}</code>
                  </pre>
                </div>
                <div className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
                  <CopyButton text={value} size="xs" />
                </div>
              </div>
            )
          },
          table({ children }) {
            return (
              <div className="overflow-x-auto my-3 rounded-lg border border-border/60">
                <table className="md-table">{children}</table>
              </div>
            )
          },
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  )
}

function ToolTrace({ trace }) {
  const [open, setOpen] = useState(false)
  if (!trace?.length) return null

  return (
    <div className="mt-3 border border-border/40 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-3 py-2 text-2xs text-ink-muted hover:text-ink-secondary hover:bg-elevated/40 transition-colors"
      >
        <span className="flex items-center gap-1.5">
          <span className="text-violet-text">⊡</span>
          <span className="font-medium uppercase tracking-wider">
            {trace.length} tool call{trace.length !== 1 ? 's' : ''}
          </span>
        </span>
        <span>{open ? '▲' : '▼'}</span>
      </button>

      {open && (
        <div className="border-t border-border/40 divide-y divide-border/30">
          {trace.map((t, i) => (
            <div key={i} className="px-3 py-2.5 bg-base/40">
              <p className="text-2xs font-semibold text-cyan-text font-mono mb-1">
                {t.tool}
              </p>
              {t.input && (
                <p className="text-2xs text-ink-muted font-mono break-all leading-relaxed">
                  → {t.input}
                </p>
              )}
              {t.output && (
                <p className="text-2xs text-ink-secondary/70 font-mono break-all leading-relaxed mt-0.5 line-clamp-3">
                  ← {t.output}
                </p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function MessageBubble({ message }) {
  const { role, content, agent, toolTrace, streaming, timestamp } = message
  const isUser = role === 'user'
  const isError = agent === 'error'

  const time = timestamp
    ? new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    : ''

  if (isUser) {
    return (
      <div className="flex justify-end px-4 md:px-6 py-2 animate-slide-up">
        <div className="max-w-[78%] md:max-w-[65%]">
          <div className="bg-elevated border border-border/70 rounded-2xl rounded-br-md px-4 py-3">
            <p className="text-sm text-ink-primary leading-relaxed whitespace-pre-wrap">
              {content}
            </p>
          </div>
          <p className="text-2xs text-ink-muted mt-1 text-right pr-1">{time}</p>
        </div>
      </div>
    )
  }

  const parts = parseContent(content)

  return (
    <div className="flex items-start gap-3 px-4 md:px-6 py-3 animate-slide-up group">
      {/* Agent avatar */}
      <div className="w-7 h-7 rounded-full shrink-0 mt-0.5 flex items-center justify-center text-xs glass-elevated">
        <span className="text-cyan-text/60">◈</span>
      </div>

      {/* Bubble */}
      <div className="flex-1 min-w-0 max-w-3xl">
        <AgentBadge agentId={agent} />

        <div
          className={`relative rounded-2xl rounded-tl-md px-4 py-3.5 ${
            isError
              ? 'bg-red-500/5 border border-red-500/20'
              : 'glass-elevated'
          }`}
        >
          {/* Copy button — top right, shows on hover */}
          {!streaming && content && (
            <div className="absolute top-2.5 right-2.5 opacity-0 group-hover:opacity-100 transition-opacity">
              <CopyButton text={content} size="xs" />
            </div>
          )}

          {/* Content */}
          {parts.map((p, i) =>
            p.type === 'plotly'
              ? <PlotlyChart key={i} json={p.content} />
              : <MarkdownContent key={i} text={p.content} />
          )}

          {/* Streaming cursor */}
          {streaming && (
            <span className="stream-cursor" />
          )}
        </div>

        {/* Tool trace + timestamp row */}
        <div className="mt-1 px-1">
          <ToolTrace trace={toolTrace} />
          <p className="text-2xs text-ink-muted mt-1">{time}</p>
        </div>
      </div>
    </div>
  )
}
