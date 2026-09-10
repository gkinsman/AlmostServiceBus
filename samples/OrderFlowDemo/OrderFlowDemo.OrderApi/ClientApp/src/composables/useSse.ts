import { ref, onUnmounted } from 'vue'

export interface DashboardEvent {
  type: 'saga-transition' | 'message-consumed' | 'message-dead-lettered'
  orderId?: string
  fromState?: string
  toState?: string
  warehouse?: string
  customerName?: string
  products?: string[]
  amount?: number
  queueName?: string
  failureReason?: string
  timestamp: string
}

export function useSse(url: string) {
  const connected = ref(false)
  const lastEvent = ref<DashboardEvent | null>(null)
  const handlers: Array<(event: DashboardEvent) => void> = []
  const openHandlers: Array<() => void> = []

  const eventSource = new EventSource(url)

  eventSource.onopen = () => {
    connected.value = true
    // Fires on the initial connect and after every automatic reconnect. Anything sent while
    // we were disconnected is gone (SSE has no replay here), so listeners resync from the API.
    openHandlers.forEach(h => h())
  }
  eventSource.onerror = () => { connected.value = false }

  eventSource.onmessage = (e) => {
    const event: DashboardEvent = JSON.parse(e.data)
    lastEvent.value = event
    handlers.forEach(h => h(event))
  }

  function onEvent(handler: (event: DashboardEvent) => void) {
    handlers.push(handler)
  }

  function onOpen(handler: () => void) {
    openHandlers.push(handler)
  }

  onUnmounted(() => {
    eventSource.close()
  })

  return { connected, lastEvent, onEvent, onOpen }
}
