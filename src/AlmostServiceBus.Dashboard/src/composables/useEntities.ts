import { ref, computed, reactive, onUnmounted } from 'vue'
import type { EntityOverview, QueueInfo, TopicInfo, SubscriptionInfo, EntityGroup, NamespaceInfo, MessageEvent } from '../types'
import type { NamespaceSse } from './useNamespaceSse'
import { api } from '../api/client'

export function useEntities(selectedNamespace: () => string, sse: NamespaceSse) {
  const namespaces = ref<NamespaceInfo[]>([])
  const entities = ref<EntityOverview | null>(null)
  const loading = ref(false)
  const filter = ref('')
  const showAll = ref(false)
  const collapsedGroups = reactive(new Set<string>())

  async function refreshNamespaces() {
    try { namespaces.value = await api.getNamespaces() } catch {}
  }

  async function refreshEntities() {
    const ns = selectedNamespace()
    if (!ns) return
    const isInitialLoad = entities.value === null
    if (isInitialLoad) loading.value = true
    try {
      entities.value = await api.getEntities(ns)
    } catch {}
    finally { if (isInitialLoad) loading.value = false }
  }

  /** Resolves a "topic/subscriptions/sub" entity path to the subscription it names, if loaded. */
  function findSubscription(entityPath: string): SubscriptionInfo | undefined {
    const marker = '/subscriptions/'
    const idx = entityPath.toLowerCase().lastIndexOf(marker)
    if (idx <= 0 || !entities.value) return undefined
    const topicName = entityPath.slice(0, idx)
    const subName = entityPath.slice(idx + marker.length)
    const topic = entities.value.topics.find(t => t.name.toLowerCase() === topicName.toLowerCase())
    return topic?.subscriptions.find(s => s.name.toLowerCase() === subName.toLowerCase())
  }

  // Apply SSE event deltas to local entity counts for real-time updates.
  function handleEvent(evt: MessageEvent) {
    // New namespace — refresh the namespace list immediately
    if (evt.type === 'NamespaceCreated') {
      refreshNamespaces()
      return
    }

    if (!entities.value) return

    const queue = entities.value.queues.find(q => q.name === evt.entity)
    if (queue) {
      if (evt.type === 'Enqueued') {
        queue.totalMessageCount++
      } else if (evt.type === 'Completed') {
        queue.consumedCount++
      } else if (evt.type === 'DeadLettered') {
        queue.deadLetterCount++
      }

      // Update subscription badges for any subscription forwarding to this queue
      for (const topic of entities.value.topics) {
        for (const sub of topic.subscriptions) {
          if (sub.forwardTo === evt.entity && evt.type === 'Enqueued') {
            sub.messageCount++
          }
        }
      }

      // Force reactivity — reassign so computed properties re-evaluate.
      entities.value = { ...entities.value }
      return
    }

    // A subscription's own queue (no ForwardTo) publishes under "topic/subscriptions/sub".
    const sub = findSubscription(evt.entity)
    if (sub) {
      if (evt.type === 'Enqueued') {
        sub.messageCount++
      } else if (evt.type === 'Completed') {
        sub.messageCount = Math.max(0, sub.messageCount - 1)
      } else if (evt.type === 'DeadLettered') {
        sub.messageCount = Math.max(0, sub.messageCount - 1)
        sub.deadLetterCount++
      }
      entities.value = { ...entities.value }
      return
    }

    // Unknown entity — a new queue/topic was created. Re-fetch entities and namespaces.
    if (evt.type === 'Enqueued') {
      refreshEntities()
      refreshNamespaces()
    }
  }

  let unsubscribe: (() => void) | null = null

  function start() {
    refreshNamespaces()
    refreshEntities()
    unsubscribe = sse.subscribe(handleEvent)
  }

  function stop() {
    unsubscribe?.()
    unsubscribe = null
  }

  function onNamespaceChange() {
    entities.value = null
    refreshNamespaces()
    refreshEntities()
  }

  function toggleGroup(prefix: string) {
    if (collapsedGroups.has(prefix)) collapsedGroups.delete(prefix)
    else collapsedGroups.add(prefix)
  }

  function isCollapsed(prefix: string) {
    return collapsedGroups.has(prefix)
  }

  function hasActivity(q: QueueInfo): boolean {
    return q.totalMessageCount > 0 || q.deadLetterCount > 0
  }

  function topicHasMessages(t: TopicInfo): boolean {
    return t.subscriptions.some(s => s.messageCount > 0 || s.deadLetterCount > 0)
  }

  const filteredQueues = computed<QueueInfo[]>(() => {
    if (!entities.value) return []
    let queues = entities.value.queues
    const f = filter.value.toLowerCase()
    if (f) queues = queues.filter(q => q.name.toLowerCase().includes(f))
    if (!showAll.value) queues = queues.filter(hasActivity)
    return queues
  })

  const topicGroups = computed<EntityGroup[]>(() => {
    if (!entities.value) return []
    let topics = entities.value.topics
    if (!showAll.value) topics = topics.filter(topicHasMessages)

    const groups = new Map<string, TopicInfo[]>()
    for (const topic of topics) {
      const slashIdx = topic.name.lastIndexOf('/')
      const prefix = slashIdx > 0 ? topic.name.substring(0, slashIdx) : ''
      if (!groups.has(prefix)) groups.set(prefix, [])
      groups.get(prefix)!.push(topic)
    }
    return Array.from(groups.entries()).map(([prefix, t]) => ({
      prefix: prefix || '(ungrouped)',
      topics: t,
      collapsed: collapsedGroups.has(prefix || '(ungrouped)'),
    }))
  })

  const totalQueues = computed(() => entities.value?.queues.length ?? 0)
  const totalTopics = computed(() => entities.value?.topics.length ?? 0)

  return {
    namespaces, entities, loading, filter, showAll, topicGroups, filteredQueues,
    totalQueues, totalTopics,
    toggleGroup, isCollapsed, start, stop, onNamespaceChange, refreshEntities,
  }
}
