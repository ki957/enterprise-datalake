import React, { useEffect, Suspense, lazy } from 'react'
import { useStore } from './store'
import { useKeyboard } from './hooks/useKeyboard'
import { useToastAutoDismiss } from './hooks/useToast'

import Header       from './components/layout/Header'
import Sidebar      from './components/layout/Sidebar'
import ServicePanel from './components/layout/ServicePanel'
import MobileDrawer from './components/layout/MobileDrawer'
import ChatArea     from './components/chat/ChatArea'
import ToastStack      from './components/ui/Toast'
import CommandPalette   from './components/ui/CommandPalette'
const CostDashboard = lazy(() => import('./components/ui/CostDashboard'))

export default function App() {
  const { openCostDashboard, setActiveAgent } = useStore()

  useKeyboard()
  useToastAutoDismiss()

  useEffect(() => {
    // Handle PWA shortcuts in query string
    const params = new URLSearchParams(window.location.search)
    if (params.get('costs')) openCostDashboard()
    const agent = params.get('agent')
    if (agent) setActiveAgent(agent)
  }, [openCostDashboard, setActiveAgent])

  return (
    <div className="flex flex-col h-screen bg-base overflow-hidden">
      <Header />

      <div className="flex flex-1 overflow-hidden">
        {/* Left sidebar — hidden on mobile, visible md+ */}
        <aside className="hidden md:flex flex-col w-[220px] shrink-0 border-r border-border/50">
          <Sidebar />
        </aside>

        {/* Chat area — always full width on mobile */}
        <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
          <ChatArea />
        </main>

        {/* Right service panel — only xl+ (Samsung Tab S9 FE+ landscape, PC) */}
        <aside className="hidden xl:flex flex-col w-[260px] shrink-0 border-l border-border/50">
          <ServicePanel />
        </aside>
      </div>

      {/* Mobile bottom drawer (sidebar for mobile) */}
      <MobileDrawer />

      {/* Global UI overlays */}
      <ToastStack />
      <CommandPalette />
      <Suspense fallback={null}>
        <CostDashboard />
      </Suspense>
    </div>
  )
}
