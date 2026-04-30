import { useEffect } from 'react'
import { useStore } from '../store'

export function useKeyboard() {
  const { openCommandPalette, closeCommandPalette, isCommandPaletteOpen, newSession } = useStore()

  useEffect(() => {
    const handler = async (e) => {
      // Cmd+K / Ctrl+K — open command palette
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        isCommandPaletteOpen ? closeCommandPalette() : openCommandPalette()
      }
      // Escape — close command palette
      if (e.key === 'Escape' && isCommandPaletteOpen) {
        closeCommandPalette()
      }
      // Ctrl+Shift+N — new session
      if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === 'n') {
        e.preventDefault()
        const oldId = useStore.getState().sessionId
        try { await fetch(`/api/session?session_id=${oldId}`, { method: 'DELETE' }) } catch {}
        newSession()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [isCommandPaletteOpen, openCommandPalette, closeCommandPalette, newSession])
}
