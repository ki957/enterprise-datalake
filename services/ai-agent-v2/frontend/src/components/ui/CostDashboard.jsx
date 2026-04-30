import React, { useEffect, useState, useCallback } from 'react'
import Plot from 'react-plotly.js'
import { useStore } from '../../store'

const AGENT_COLORS = {
  insight:       '#5BC8D4',
  quality:       '#F59E0B',
  ingestion:     '#8B5CF6',
  orchestration: '#10B981',
  performance:   '#EF4444',
  schema:        '#3B82F6',
  airbyte:       '#F97316',
  nl_dbt:        '#EC4899',
  contract:      '#14B8A6',
  self_healing:  '#A855F7',
  anomaly:       '#6366F1',
  auto:          '#94A3B8',
}

function fmt$(v) {
  if (v == null) return '$—'
  const n = parseFloat(v)
  if (n < 0.0001) return '<$0.0001'
  return `$${n.toFixed(4)}`
}

function fmtTokens(v) {
  if (v == null) return '—'
  const n = parseInt(v, 10)
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}K`
  return String(n)
}

function fmtMs(v) {
  if (v == null) return '—'
  const n = parseFloat(v)
  if (n >= 1000) return `${(n / 1000).toFixed(1)}s`
  return `${Math.round(n)}ms`
}

function fmtDate(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

function fmtTime(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
}

function StatCard({ label, value, sub, accent }) {
  return (
    <div
      className="rounded-lg p-4 flex flex-col gap-1"
      style={{ background: `${accent || '#5BC8D4'}10`, border: `1px solid ${accent || '#5BC8D4'}25` }}
    >
      <span className="text-xs text-ink-muted uppercase tracking-wide">{label}</span>
      <span className="text-xl font-bold text-ink-primary">{value}</span>
      {sub && <span className="text-xs text-ink-muted">{sub}</span>}
    </div>
  )
}

export default function CostDashboard() {
  const { isCostDashboardOpen, closeCostDashboard } = useStore()

  const [summary,  setSummary]  = useState(null)
  const [daily,    setDaily]    = useState([])
  const [recent,   setRecent]   = useState([])
  const [loading,  setLoading]  = useState(false)
  const [days,     setDays]     = useState(14)
  const [tab,      setTab]      = useState('overview')

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const [s, d, r] = await Promise.all([
        fetch(`/api/costs/summary?days=${days}`).then(r => r.json()),
        fetch(`/api/costs/daily?days=${days}`).then(r => r.json()),
        fetch(`/api/costs/recent?limit=50`).then(r => r.json()),
      ])
      setSummary(s)
      setDaily(d.rows || [])
      setRecent(r.rows || [])
    } catch {
      /* API may not be running yet */
    }
    setLoading(false)
  }, [days])

  useEffect(() => {
    if (isCostDashboardOpen) load()
  }, [isCostDashboardOpen, load])

  if (!isCostDashboardOpen) return null

  // Build daily spend chart data
  const agentNames = [...new Set(daily.map(r => r.agent))]
  const dateSet    = [...new Set(daily.map(r => r.day))].sort()

  const plotTraces = agentNames.map(agent => {
    const costByDate = {}
    daily.filter(r => r.agent === agent).forEach(r => {
      costByDate[r.day] = parseFloat(r.total_cost || 0)
    })
    return {
      type:        'bar',
      name:        agent,
      x:           dateSet,
      y:           dateSet.map(d => costByDate[d] || 0),
      marker:      { color: AGENT_COLORS[agent] || '#5BC8D4' },
      hovertemplate: `%{y:.5f} USD<br>%{x}<extra>${agent}</extra>`,
    }
  })

  const totals    = summary?.totals    || {}
  const by_agent  = summary?.by_agent  || []

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: 'rgba(0,0,0,0.7)', backdropFilter: 'blur(4px)' }}
      onClick={e => e.target === e.currentTarget && closeCostDashboard()}
    >
      <div
        className="relative flex flex-col w-full max-w-4xl max-h-[90vh] rounded-xl overflow-hidden"
        style={{ background: 'var(--color-elevated)', border: '1px solid var(--color-border)' }}
      >
        {/* ── Header ── */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-border/50 shrink-0">
          <div>
            <h2 className="text-base font-semibold text-ink-primary">Cost Dashboard</h2>
            <p className="text-xs text-ink-muted mt-0.5">
              Token spend · Groq llama-4-scout · Input $0.11/M · Output $0.34/M
            </p>
          </div>
          <div className="flex items-center gap-3">
            {/* Day range selector */}
            <select
              value={days}
              onChange={e => setDays(Number(e.target.value))}
              className="text-xs px-2 py-1 rounded-md bg-base border border-border text-ink-secondary"
            >
              <option value={7}>Last 7 days</option>
              <option value={14}>Last 14 days</option>
              <option value={30}>Last 30 days</option>
            </select>
            <button
              onClick={closeCostDashboard}
              className="flex items-center justify-center w-7 h-7 rounded-md hover:bg-base transition-colors text-ink-muted hover:text-ink-primary"
            >
              ✕
            </button>
          </div>
        </div>

        {/* ── Tabs ── */}
        <div className="flex gap-1 px-6 pt-3 pb-0 border-b border-border/30 shrink-0">
          {['overview', 'daily', 'calls'].map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-3 py-1.5 text-xs font-medium rounded-t-md capitalize transition-colors ${
                tab === t
                  ? 'text-cyan-text border-b-2 border-cyan-text'
                  : 'text-ink-muted hover:text-ink-secondary'
              }`}
            >
              {t === 'overview' ? 'Overview' : t === 'daily' ? 'Daily Spend' : 'Call Log'}
            </button>
          ))}
        </div>

        {/* ── Body ── */}
        <div className="flex-1 overflow-y-auto px-6 py-5">
          {loading ? (
            <div className="flex items-center justify-center h-32 text-ink-muted text-sm">
              Loading cost data…
            </div>
          ) : (

            // ── Overview ──────────────────────────────────────────────────────
            tab === 'overview' ? (
              <div className="flex flex-col gap-6">
                {/* Stat cards */}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  <StatCard
                    label="Total Spend"
                    value={fmt$(totals.total_cost_usd)}
                    sub={`${days}-day window`}
                    accent="#5BC8D4"
                  />
                  <StatCard
                    label="Total Calls"
                    value={totals.total_calls ?? '—'}
                    sub={`input + output rounds`}
                    accent="#10B981"
                  />
                  <StatCard
                    label="Input Tokens"
                    value={fmtTokens(totals.total_input_tokens)}
                    sub={`@ $0.11/M`}
                    accent="#8B5CF6"
                  />
                  <StatCard
                    label="Output Tokens"
                    value={fmtTokens(totals.total_output_tokens)}
                    sub={`@ $0.34/M`}
                    accent="#F59E0B"
                  />
                </div>

                {/* Per-agent breakdown */}
                <div>
                  <h3 className="text-xs font-semibold text-ink-secondary uppercase tracking-wide mb-3">
                    Per-Agent Breakdown
                  </h3>
                  {by_agent.length === 0 ? (
                    <p className="text-sm text-ink-muted">No calls recorded yet in this window.</p>
                  ) : (
                    <div className="flex flex-col gap-2">
                      {by_agent.map(row => {
                        const pct = totals.total_cost_usd
                          ? (parseFloat(row.total_cost) / parseFloat(totals.total_cost_usd)) * 100
                          : 0
                        const color = AGENT_COLORS[row.agent] || '#5BC8D4'
                        return (
                          <div key={row.agent} className="flex items-center gap-3">
                            <span
                              className="w-2.5 h-2.5 rounded-full shrink-0"
                              style={{ background: color }}
                            />
                            <span className="w-28 text-xs text-ink-secondary capitalize truncate">
                              {row.agent}
                            </span>
                            <div className="flex-1 h-1.5 rounded-full bg-border/40 overflow-hidden">
                              <div
                                className="h-full rounded-full transition-all duration-500"
                                style={{ width: `${pct.toFixed(1)}%`, background: color }}
                              />
                            </div>
                            <span className="w-20 text-right text-xs text-ink-primary font-mono">
                              {fmt$(row.total_cost)}
                            </span>
                            <span className="w-14 text-right text-xs text-ink-muted">
                              {row.calls} calls
                            </span>
                            <span className="w-20 text-right text-xs text-ink-muted">
                              avg {fmtMs(row.avg_latency_ms)}
                            </span>
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
              </div>

            // ── Daily Spend chart ─────────────────────────────────────────────
            ) : tab === 'daily' ? (
              <div>
                {plotTraces.length === 0 ? (
                  <p className="text-sm text-ink-muted">No data yet — ask the agents some questions first.</p>
                ) : (
                  <Plot
                    data={plotTraces}
                    layout={{
                      barmode:   'stack',
                      paper_bgcolor: 'transparent',
                      plot_bgcolor:  'transparent',
                      font:      { color: '#94A3B8', size: 11 },
                      margin:    { t: 16, r: 16, b: 48, l: 56 },
                      xaxis:     { gridcolor: '#1e293b', tickfont: { size: 10 } },
                      yaxis:     {
                        gridcolor: '#1e293b',
                        title: { text: 'USD', font: { size: 11 } },
                        tickformat: '.5f',
                      },
                      legend:    { orientation: 'h', y: -0.2 },
                      showlegend: true,
                    }}
                    config={{ displayModeBar: false, responsive: true }}
                    style={{ width: '100%', height: 320 }}
                  />
                )}
              </div>

            // ── Call Log ──────────────────────────────────────────────────────
            ) : (
              <div>
                {recent.length === 0 ? (
                  <p className="text-sm text-ink-muted">No calls recorded yet.</p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs border-collapse">
                      <thead>
                        <tr className="border-b border-border/40">
                          {['Time', 'Agent', 'In Tokens', 'Out Tokens', 'Cost', 'Latency'].map(h => (
                            <th key={h} className="py-2 px-3 text-left font-semibold text-ink-muted uppercase tracking-wide">
                              {h}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {recent.map(row => (
                          <tr
                            key={row.id}
                            className="border-b border-border/20 hover:bg-base/30 transition-colors"
                          >
                            <td className="py-2 px-3 text-ink-muted font-mono whitespace-nowrap">
                              {fmtDate(row.created_at)} {fmtTime(row.created_at)}
                            </td>
                            <td className="py-2 px-3">
                              <span
                                className="px-2 py-0.5 rounded text-xs font-medium capitalize"
                                style={{
                                  background: `${AGENT_COLORS[row.agent] || '#5BC8D4'}18`,
                                  color:       AGENT_COLORS[row.agent] || '#5BC8D4',
                                }}
                              >
                                {row.agent}
                              </span>
                            </td>
                            <td className="py-2 px-3 text-ink-secondary font-mono">
                              {fmtTokens(row.input_tokens)}
                            </td>
                            <td className="py-2 px-3 text-ink-secondary font-mono">
                              {fmtTokens(row.output_tokens)}
                            </td>
                            <td className="py-2 px-3 text-ink-primary font-mono font-medium">
                              {fmt$(row.total_cost)}
                            </td>
                            <td className="py-2 px-3 text-ink-muted font-mono">
                              {fmtMs(row.latency_ms)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )
          )}
        </div>
      </div>
    </div>
  )
}
