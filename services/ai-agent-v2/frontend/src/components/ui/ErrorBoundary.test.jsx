import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ErrorBoundary } from '../components/ui/ErrorBoundary'

// Component that throws on render
const BrokenComponent = () => {
  throw new Error('Test error')
}

describe('ErrorBoundary', () => {
  it('renders children when no error occurs', () => {
    render(
      <ErrorBoundary>
        <div data-testid="child">Hello World</div>
      </ErrorBoundary>
    )
    expect(screen.getByTestId('child')).toBeInTheDocument()
    expect(screen.getByText('Hello World')).toBeInTheDocument()
  })

  it('displays fallback UI when child throws', () => {
    // Suppress console.error for this test
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    render(
      <ErrorBoundary>
        <BrokenComponent />
      </ErrorBoundary>
    )

    expect(screen.getByText('Something went wrong')).toBeInTheDocument()
    expect(screen.getByText('The application encountered an unexpected error.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /reload application/i })).toBeInTheDocument()

    consoleSpy.mockRestore()
  })

  it('provides a reload button that resets state', () => {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const reloadSpy = vi.fn()
    // Mock window.location.reload
    Object.defineProperty(window, 'location', {
      value: { reload: reloadSpy },
      writable: true,
    })

    render(
      <ErrorBoundary>
        <BrokenComponent />
      </ErrorBoundary>
    )

    const reloadButton = screen.getByRole('button', { name: /reload application/i })
    fireEvent.click(reloadButton)
    expect(reloadSpy).toHaveBeenCalled()

    consoleSpy.mockRestore()
  })
})
