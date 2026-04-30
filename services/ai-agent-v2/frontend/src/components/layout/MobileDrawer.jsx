import React, { useEffect } from 'react'
import { useStore } from '../../store'
import { SERVICES, resolveUrl } from '../../data/services'
import Sidebar from './Sidebar'

export default function MobileDrawer() {
  const { isMobileSidebarOpen, closeMobileSidebar } = useStore()

  // Lock scroll when open
  useEffect(() => {
    document.body.style.overflow = isMobileSidebarOpen ? 'hidden' : ''
    return () => { document.body.style.overflow = '' }
  }, [isMobileSidebarOpen])

  if (!isMobileSidebarOpen) return null

  return (
    <>
      {/* Backdrop */}
      <div className="drawer-overlay md:hidden" onClick={closeMobileSidebar} />

      {/* Panel */}
      <div className="drawer-panel md:hidden">
        {/* Drag handle */}
        <div className="flex justify-center pt-3 pb-1">
          <div className="w-10 h-1 rounded-full bg-border" />
        </div>

        {/* Branding */}
        <div className="px-5 pt-2 pb-3 border-b border-border/50">
          <p className="text-base font-semibold text-ink-primary">Codincity</p>
          <p className="text-xs text-cyan-text">DataLake AI</p>
        </div>

        {/* Agents */}
        <div className="border-b border-border/50">
          <Sidebar onAgentSelect={closeMobileSidebar} />
        </div>

        {/* Services — compact list */}
        <div className="px-4 py-3">
          <p className="text-2xs text-ink-muted font-semibold uppercase tracking-widest mb-2">
            Services
          </p>
          <MobileServiceLinks />
        </div>
      </div>
    </>
  )
}

function MobileServiceLinks() {
  const { serviceHealth } = useStore()

  return (
    <div className="grid grid-cols-2 gap-1.5">
      {SERVICES.map(svc => {
        const status = serviceHealth[svc.id] ?? 'unknown'
        return (
          <a
            key={svc.id}
            href={resolveUrl(svc.url)}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-3 py-2 rounded-lg bg-elevated/50 hover:bg-elevated"
          >
            <span className={`status-dot ${status}`} />
            <span className="text-xs text-ink-secondary font-medium truncate">
              {svc.label}
            </span>
          </a>
        )
      })}
    </div>
  )
}
