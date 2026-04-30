import { useEffect } from 'react'
import { useStore } from '../store'

export function useToast() {
  const { addToast } = useStore()
  return { toast: addToast }
}

// Auto-dismiss toasts
export function useToastAutoDismiss() {
  const { toasts, removeToast } = useStore()

  useEffect(() => {
    toasts.forEach(t => {
      if (!t._timer) {
        t._timer = setTimeout(() => removeToast(t.id), t.duration ?? 4000)
      }
    })
  }, [toasts, removeToast])
}
