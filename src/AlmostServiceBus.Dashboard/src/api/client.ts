import type { NamespaceInfo, EntityOverview, MessageInfo, EmulatorInfo, QueueProperties } from '../types'

const BASE = '/api/dashboard'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

async function del(path: string): Promise<void> {
  const res = await fetch(`${BASE}${path}`, { method: 'DELETE' })
  if (!res.ok) throw new Error(`API error: ${res.status}`)
}

/**
 * Encodes a dynamic path segment while preserving literal "/" separators —
 * entity names here are legitimately hierarchical (topic grouping prefixes,
 * "topic/subscriptions/sub" composite paths), and the backend's {**path}
 * catch-all routes match on raw slashes, so a blanket encodeURIComponent
 * would break routing.
 */
function encodePath(segment: string): string {
  return segment.split('/').map(encodeURIComponent).join('/')
}

export const api = {
  getInfo: () => get<EmulatorInfo>('/info'),

  getNamespaces: () => get<NamespaceInfo[]>('/namespaces'),

  getEntities: (ns: string) => get<EntityOverview>(`/namespaces/${encodePath(ns)}/entities`),

  getQueueMessages: (ns: string, queueName: string) =>
    get<MessageInfo[]>(`/namespaces/${encodePath(ns)}/queues/${encodePath(queueName)}/messages`),

  getTopicMessages: (ns: string, topicName: string) =>
    get<MessageInfo[]>(`/namespaces/${encodePath(ns)}/topics/${encodePath(topicName)}/messages`),

  getDeadLetterMessages: (ns: string, queueName: string) =>
    get<MessageInfo[]>(`/namespaces/${encodePath(ns)}/queues/${encodePath(queueName)}/deadletter`),

  getQueueProperties: (ns: string, queueName: string) =>
    get<QueueProperties>(`/namespaces/${encodePath(ns)}/queues/${encodePath(queueName)}/properties`),

  purgeQueue: (ns: string, queueName: string) =>
    del(`/namespaces/${encodePath(ns)}/queues/${encodePath(queueName)}/messages`),

  purgeDeadLetter: (ns: string, queueName: string) =>
    del(`/namespaces/${encodePath(ns)}/queues/${encodePath(queueName)}/deadletter`),

  /** entityPath is the composite "topicName/subscriptions/subName" path. */
  getSubscriptionMessages: (ns: string, entityPath: string) =>
    get<MessageInfo[]>(`/namespaces/${encodePath(ns)}/topics/${encodePath(entityPath)}/messages`),

  getSubscriptionDeadLetterMessages: (ns: string, entityPath: string) =>
    get<MessageInfo[]>(`/namespaces/${encodePath(ns)}/topics/${encodePath(entityPath)}/deadletter`),

  purgeSubscriptionDeadLetter: (ns: string, entityPath: string) =>
    del(`/namespaces/${encodePath(ns)}/topics/${encodePath(entityPath)}/deadletter`),
}
