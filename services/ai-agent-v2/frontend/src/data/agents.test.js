import { describe, it, expect } from 'vitest'
import { AGENTS } from '../../data/agents'

describe('Agents data', () => {
  it('has at least 10 agents defined', () => {
    expect(AGENTS.length).toBeGreaterThanOrEqual(10)
  })

  it('each agent has required fields', () => {
    AGENTS.forEach((agent) => {
      expect(agent).toHaveProperty('id')
      expect(agent).toHaveProperty('label')
      expect(agent).toHaveProperty('icon')
      expect(agent).toHaveProperty('color')
      expect(agent).toHaveProperty('placeholder')
      expect(agent).toHaveProperty('statusMessage')
    })
  })

  it('agent IDs are unique', () => {
    const ids = AGENTS.map((a) => a.id)
    const uniqueIds = new Set(ids)
    expect(ids.length).toBe(uniqueIds.size)
  })

  it('auto agent is first in the list', () => {
    expect(AGENTS[0].id).toBe('auto')
  })
})
