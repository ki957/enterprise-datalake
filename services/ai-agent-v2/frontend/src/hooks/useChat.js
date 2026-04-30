import { useCallback } from 'react'
import { useStore } from '../store'

export function useChat() {
  const {
    sessionId, activeAgent,
    addMessage, updateMessage, setThinking, addToast,
  } = useStore()

  const sendMessage = useCallback(async (text) => {
    if (!text?.trim()) return

    // Snapshot history BEFORE adding the user message, using getState() to
    // avoid stale-closure issues when messages change between renders.
    const history = useStore.getState().messages.slice(-6).map(m => ({
      role:    m.role === 'user' ? 'human' : 'assistant',
      content: m.content,
    }))

    addMessage({ role: 'user', content: text.trim() })
    setThinking(true, null, 'Routing to best agent…')

    let msgId = null
    let contentBuffer = ''

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message:    text.trim(),
          session_id: sessionId,
          agent:      activeAgent,
          history,
        }),
      })

      if (!res.ok) throw new Error(`API error ${res.status}`)

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() // keep incomplete line

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const raw = line.slice(6).trim()
          if (!raw) continue

          let evt
          try { evt = JSON.parse(raw) } catch { continue }

          if (evt.type === 'status') {
            setThinking(true, evt.agent, evt.message)

          } else if (evt.type === 'agent') {
            setThinking(false)
            msgId = addMessage({
              role:      'assistant',
              content:   '',
              agent:     evt.agent,
              toolTrace: evt.trace || [],
              streaming: true,
            })
            contentBuffer = ''

          } else if (evt.type === 'token') {
            contentBuffer += evt.content
            if (msgId) updateMessage(msgId, { content: contentBuffer })

          } else if (evt.type === 'done') {
            if (msgId) updateMessage(msgId, { content: contentBuffer, streaming: false })
            contentBuffer = ''
            msgId = null

          } else if (evt.type === 'error') {
            setThinking(false)
            addMessage({
              role:    'assistant',
              content: `⚠ ${evt.message}`,
              agent:   'error',
            })
            addToast({ type: 'error', message: evt.message })
          }
        }
      }
    } catch (err) {
      const errMsg = `Connection error: ${err.message}. Make sure the API server is running on :8502`
      if (msgId) {
        updateMessage(msgId, { content: errMsg, streaming: false, agent: 'error' })
      } else {
        addMessage({ role: 'assistant', content: errMsg, agent: 'error' })
      }
      addToast({ type: 'error', message: 'API server unreachable' })
    } finally {
      // Guarantee thinking state is always cleared, even if stream ends
      // without an 'agent' event (e.g. network drop mid-stream).
      setThinking(false)
      if (msgId) updateMessage(msgId, { streaming: false })
    }
  }, [sessionId, activeAgent, addMessage, updateMessage, setThinking, addToast])

  return { sendMessage }
}
