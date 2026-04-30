import React, { useEffect } from 'react'
import { useStore } from '../../store'
import { SERVICES, TIER_LABELS, resolveUrl } from '../../data/services'
import clsx from 'clsx'

// Fetch and cache health every 5 minutes
async function fetchHealth(setServiceHealth) {
  try {
    const res = await fetch('/api/health')
    if (!res.ok) return
    const data = await res.json()
    setServiceHealth(data.services ?? {})
  } catch {
    // API not running — leave as unknown
  }
}

export default function ServicePanel() {
  const { serviceHealth, setServiceHealth } = useStore()

  useEffect(() => {
    fetchHealth(setServiceHealth)
    const id = setInterval(() => fetchHealth(setServiceHealth), 5 * 60 * 1000)
    return () => clearInterval(id)
  }, [setServiceHealth])

  const tiers = [1, 2, 3]

  return (
    <div className="flex flex-col h-full py-4 overflow-y-auto px-3">
      <p className="text-2xs text-ink-muted font-semibold uppercase tracking-widest mb-3 px-1">
        Services
      </p>

      {tiers.map(tier => {
        const services = SERVICES.filter(s => s.tier === tier)
        return (
          <div key={tier} className="mb-4">
            <p className="text-2xs text-ink-muted/60 font-medium px-1 mb-1.5">
              {TIER_LABELS[tier]}
            </p>
            <div className="flex flex-col gap-0.5">
              {services.map(svc => {
                const status = serviceHealth[svc.id] ?? 'unknown'
                return (
                  <a
                    key={svc.id}
                    href={resolveUrl(svc.url)}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-2.5 px-2.5 py-2 rounded-lg hover:bg-elevated transition-colors duration-150 group"
                  >
                    <span className={`status-dot ${status} shrink-0`} />
                    <span className="flex-1 min-w-0">
                      <span className="text-sm text-ink-secondary group-hover:text-ink-primary transition-colors font-medium block truncate">
                        {svc.label}
                      </span>
                      <span className="text-2xs text-ink-muted truncate block">
                        :{svc.port}
                      </span>
                    </span>
                    <span className="text-ink-muted/40 group-hover:text-ink-muted text-xs transition-colors">
                      ↗
                    </span>
                  </a>
                )
              })}
            </div>
            {tier < 3 && <div className="divider mt-3" />}
          </div>
        )
      })}

      {/* Refresh button */}
      <div className="mt-auto pt-2">
        <div className="divider mb-3" />
        <button
          onClick={() => fetchHealth(setServiceHealth)}
          className="w-full flex items-center justify-center gap-2 py-2 px-3 rounded-lg text-xs text-ink-muted hover:text-ink-secondary hover:bg-elevated transition-all duration-150"
        >
          <span>↺</span>
          <span>Refresh status</span>
        </button>
      </div>
    </div>
  )
}
