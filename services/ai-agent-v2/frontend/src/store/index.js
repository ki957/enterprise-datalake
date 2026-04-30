import { create } from 'zustand'
import { v4 as uuidv4 } from 'uuid'

const SESSION_KEY = 'datalake-session-id'
function createFreshSessionId() {
  const id = uuidv4()
  try { localStorage.setItem(SESSION_KEY, id) } catch {}
  return id
}

export const useStore = create((set, get) => ({
  // ── Session ────────────────────────────────────────────────────────────────
  sessionId: createFreshSessionId(),

  newSession: () => {
    const id = uuidv4()
    try { localStorage.setItem(SESSION_KEY, id) } catch {}
    set({
      sessionId: id,
      messages: [],
      isThinking: false,
      thinkingAgent: null,
      thinkingMsg: '',
      activeAgent: 'auto',
    })
  },

  // ── Messages ───────────────────────────────────────────────────────────────
  messages: [],

  addMessage: (msg) => {
    const id = uuidv4()
    set(s => ({ messages: [...s.messages, { id, timestamp: Date.now(), ...msg }] }))
    return id
  },

  updateMessage: (id, updates) => set(s => ({
    messages: s.messages.map(m => m.id === id ? { ...m, ...updates } : m),
  })),

  clearMessages: () => set({ messages: [] }),

  setMessages: (messages) => set({ messages }),

  // ── Agent routing ──────────────────────────────────────────────────────────
  activeAgent: 'auto',
  prevAgent: null,
  setActiveAgent: (agent) => set(s => ({
    prevAgent: s.activeAgent,
    activeAgent: agent,
  })),

  // ── Thinking / loading ─────────────────────────────────────────────────────
  isThinking: false,
  thinkingAgent: null,
  thinkingMsg: '',
  setThinking: (isThinking, agent = null, msg = '') => set({
    isThinking, thinkingAgent: agent, thinkingMsg: msg,
  }),

  // ── UI state ───────────────────────────────────────────────────────────────
  isMobileSidebarOpen: false,
  isCommandPaletteOpen: false,
  isQuickStartOpen: false,
  isCostDashboardOpen: false,
  toggleMobileSidebar: () => set(s => ({ isMobileSidebarOpen: !s.isMobileSidebarOpen })),
  closeMobileSidebar: () => set({ isMobileSidebarOpen: false }),
  openCommandPalette: () => set({ isCommandPaletteOpen: true }),
  closeCommandPalette: () => set({ isCommandPaletteOpen: false }),
  toggleQuickStart: () => set(s => ({ isQuickStartOpen: !s.isQuickStartOpen })),
  closeQuickStart: () => set({ isQuickStartOpen: false }),
  openCostDashboard:  () => set({ isCostDashboardOpen: true }),
  closeCostDashboard: () => set({ isCostDashboardOpen: false }),

  // ── Service health (populated by useHealth hook) ───────────────────────────
  serviceHealth: {},
  setServiceHealth: (health) => set({ serviceHealth: health }),

  // ── Toasts ─────────────────────────────────────────────────────────────────
  toasts: [],
  addToast: (toast) => set(s => ({
    toasts: [...s.toasts, { id: uuidv4(), duration: 4000, ...toast }],
  })),
  removeToast: (id) => set(s => ({
    toasts: s.toasts.filter(t => t.id !== id),
  })),
}))
