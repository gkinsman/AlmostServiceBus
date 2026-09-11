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
 * Entity names are hierarchical ("OrderFlowDemo/Contracts/OrderSubmitted") but each name is a
 * single segment of the API route, so its slashes travel as %2F. encodeURIComponent does that.
 */
const seg = encodeURIComponent

const queueUrl = (ns: string, queue: string) => `/namespaces/${seg(ns)}/queues/${seg(queue)}`
const topicUrl = (ns: string, topic: string) => `/namespaces/${seg(ns)}/topics/${seg(topic)}`

/**
 * The dashboard tracks a subscription as the composite "topicName/subscriptions/subName" path
 * (the same address the SDK uses). The API addresses it as two segments. Subscription names
 * cannot contain "/", so the last "/subscriptions/" marker is the split point.
 */
function subscriptionUrl(ns: string, entityPath: string): string {
  const marker = '/subscriptions/'
  const idx = entityPath.lastIndexOf(marker)
  if (idx < 0) throw new Error(`not a subscription path: ${entityPath}`)
  const topic = entityPath.slice(0, idx)
  const subscription = entityPath.slice(idx + marker.length)
  return `${topicUrl(ns, topic)}/subscriptions/${seg(subscription)}`
}

export const api = {
  getInfo: () => get<EmulatorInfo>('/info'),

  getNamespaces: () => get<NamespaceInfo[]>('/namespaces'),

  getEntities: (ns: string) => get<EntityOverview>(`/namespaces/${seg(ns)}/entities`),

  getQueueMessages: (ns: string, queue: string) => get<MessageInfo[]>(`${queueUrl(ns, queue)}/messages`),

  getDeadLetterMessages: (ns: string, queue: string) => get<MessageInfo[]>(`${queueUrl(ns, queue)}/deadletter`),

  getQueueProperties: (ns: string, queue: string) => get<QueueProperties>(`${queueUrl(ns, queue)}/properties`),

  purgeQueue: (ns: string, queue: string) => del(`${queueUrl(ns, queue)}/messages`),

  purgeDeadLetter: (ns: string, queue: string) => del(`${queueUrl(ns, queue)}/deadletter`),

  /** Newest messages across all of the topic's subscriptions. */
  getTopicMessages: (ns: string, topic: string) => get<MessageInfo[]>(`${topicUrl(ns, topic)}/messages`),

  /** entityPath is the composite "topicName/subscriptions/subName" path. */
  getSubscriptionMessages: (ns: string, entityPath: string) =>
    get<MessageInfo[]>(`${subscriptionUrl(ns, entityPath)}/messages`),

  getSubscriptionDeadLetterMessages: (ns: string, entityPath: string) =>
    get<MessageInfo[]>(`${subscriptionUrl(ns, entityPath)}/deadletter`),

  purgeSubscriptionDeadLetter: (ns: string, entityPath: string) =>
    del(`${subscriptionUrl(ns, entityPath)}/deadletter`),
}
