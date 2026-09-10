import { ref, onUnmounted } from 'vue'
import type { MessageInfo, MessageEvent, QueueProperties } from '../types'
import type { NamespaceSse } from './useNamespaceSse'
import { api } from '../api/client'

export function useMessages(
  ns: () => string,
  entity: () => string | null,
  entityType: () => 'queue' | 'topic' | null,
  sse: NamespaceSse,
) {
  const messages = ref<MessageInfo[]>([])
  let unsubscribe: (() => void) | null = null

  async function refresh() {
    const e = entity()
    const t = entityType()
    if (!e) { messages.value = []; return }
    try {
      messages.value = t === 'topic'
        ? await api.getTopicMessages(ns(), e)
        : await api.getQueueMessages(ns(), e)
    } catch { /* ignore */ }
  }

  function handleEvent(evt: MessageEvent) {
    const e = entity()
    if (!e || evt.entity !== e) return

    if (evt.type === 'Enqueued') {
      messages.value.unshift({
        messageId: evt.messageId,
        sequenceNumber: evt.sequenceNumber,
        contentType: evt.contentType,
        correlationId: evt.correlationId ?? null,
        deliveryCount: 0,
        enqueuedTimeUtc: evt.timestamp,
        subject: evt.subject ?? null,
        // Carried on the event since 0.5.0; rows created live used to show
        // "No application properties" until the next full refresh.
        applicationProperties: evt.applicationProperties ?? null,
        bodyText: evt.bodyPreview,
        scalarProperties: evt.scalarProperties,
        state: 'Active',
      })
      if (messages.value.length > 200) messages.value.pop()
    } else if (evt.type === 'Completed') {
      const msg = messages.value.find(m => m.messageId === evt.messageId)
      if (msg) msg.state = 'Consumed'
    } else if (evt.type === 'DeadLettered') {
      const msg = messages.value.find(m => m.messageId === evt.messageId)
      if (msg) msg.state = 'DeadLettered'
      // The DLQ list is fetched, not event-sourced; keep it current if it is being viewed.
      if (deadLetterMessages.value.length > 0 || deadLetterViewActive.value) void refreshDeadLetter()
    }
  }

  function startListening() {
    stopListening()
    unsubscribe = sse.subscribe(handleEvent)
  }

  function stopListening() {
    unsubscribe?.()
    unsubscribe = null
  }

  const deadLetterMessages = ref<MessageInfo[]>([])
  /** Set by the view while the Dead Letter tab is showing so live dead-letters refresh it. */
  const deadLetterViewActive = ref(false)

  async function refreshDeadLetter() {
    const e = entity()
    if (!e || entityType() !== 'queue') { deadLetterMessages.value = []; return }
    try {
      deadLetterMessages.value = await api.getDeadLetterMessages(ns(), e)
    } catch { /* ignore */ }
  }

  const properties = ref<QueueProperties | null>(null)

  async function refreshProperties() {
    const e = entity()
    if (!e || entityType() !== 'queue') { properties.value = null; return }
    try {
      properties.value = await api.getQueueProperties(ns(), e)
    } catch { properties.value = null }
  }

  return {
    messages, deadLetterMessages, deadLetterViewActive, properties,
    connected: sse.connected,
    refresh, refreshDeadLetter, refreshProperties, startListening, stopListening,
  }
}
