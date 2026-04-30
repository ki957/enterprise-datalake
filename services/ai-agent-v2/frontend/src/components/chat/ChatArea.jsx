import React, { useEffect, useRef } from 'react'
import { useStore } from '../../store'
import MessageBubble from './MessageBubble'
import ThinkingIndicator from './ThinkingIndicator'
import AgentSwitchDivider from './AgentSwitchDivider'
import InputBar from './InputBar'
import EmptyState from '../welcome/EmptyState'
import QuickStartPanel from './QuickStartPanel'

export default function ChatArea() {
  const { messages, isThinking } = useStore()
  const bottomRef = useRef(null)

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages.length, isThinking])

  return (
    <div className="flex flex-col h-full">
      {/* Messages */}
      <div className="flex-1 overflow-y-auto">
        {messages.length === 0 && !isThinking ? (
          <EmptyState />
        ) : (
          <div className="py-4">
            {messages.map((msg, idx) => {
              // Show agent switch divider between AI messages with different agents
              const prev = messages[idx - 1]
              const showDivider =
                msg.role === 'assistant' &&
                prev?.role === 'assistant' &&
                prev?.agent &&
                msg.agent &&
                prev.agent !== msg.agent

              return (
                <React.Fragment key={msg.id}>
                  {showDivider && (
                    <AgentSwitchDivider
                      fromAgent={prev.agent}
                      toAgent={msg.agent}
                    />
                  )}
                  <MessageBubble message={msg} />
                </React.Fragment>
              )
            })}

            <ThinkingIndicator />
            <div ref={bottomRef} />
          </div>
        )}
      </div>

      {/* Quick start panel — slides in above input when toggled */}
      <QuickStartPanel />

      {/* Input */}
      <InputBar />
    </div>
  )
}
